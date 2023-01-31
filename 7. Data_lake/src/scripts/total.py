
import sys
import findspark
findspark.init()
findspark.find()
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
 
import datetime
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window 
import pyspark.sql.functions as F
import pandas as pd
 

spark = (
    SparkSession
    .builder
    .master('yarn')
    #.master("local[10]")
    .config("spark.driver.memory", "5g")
    .config("spark.driver.cores", 25)
    .config("spark.executor.memory", "2g") 
    .config("spark.executor.cores", 2)
    .appName("mp_project_session")
    .getOrCreate()
)

# df с геоданными по городам
# Список городов, по которым Spark распознаёт таймзоны 
timezone_cities = ["Darwin", "Perth", "Eucla", "Brisbane", "Lindeman", "Canberra", "Queensland", 
           "Adelaide", "Hobart", "Melbourne", "Sydney", "Broken_Hill", "Lord_Howe"]
city_temp = spark.read.csv(path = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/pavelunkno/citygeodata/", 
                    sep=";", header=True)
city = (
    city_temp
    .withColumn("lat", F.regexp_replace("lat", ",", ".")) 
    .withColumn("lng", F.regexp_replace("lng", ",", ".")) 
    .withColumn("lat", F.col("lat").cast("double")) 
    .withColumn("lng", F.col("lng").cast("double"))
    .withColumn("tz_city", F.when(F.col("city").isin(timezone_cities), F.col("city"))) 
    .withColumn('city_tz', 
    F.expr("CASE WHEN city in ('Gold Coast','Townsville','Ipswich','Cairns','Toowoomba','Mackay','Rockhampton') then 'Brisbane'" + 
          "WHEN city in ('Newcastle','Wollongong','Geelong','Ballarat','Bendigo','Maitland','Cranbourne') then 'Sydney'" +
           "when city in ('Bunbury') then 'Perth'" +
           "when city in ('Launceston') then 'Hobart'else tz_city end"))
    .withColumn("tz", F.concat(F.lit("Australia/"),F.col("city_tz")))
    .drop('tz_city', 'city_tz')
    .withColumnRenamed('lat', 'lat_city') # меняем названия для будущего джойна
)



# функция для генерации путей по глубине и дате
def input_event_paths(base_path,date,depth):

    if date is None and depth is None:
        return [f"{base_path}/event_type={event_type}" for event_type in ['message','reaction','subscription']]
    else:
        dt = datetime.datetime.strptime(date, '%Y-%m-%d')
        return [f"{base_path}/event_type={event_type}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for event_type in ['message','reaction','subscription'] for x in range(depth)]


# берем небольшую глубину для быстроты
base_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/pavelunkno/data/geo/events'
date = '2022-05-30'
depth = 5
p = input_event_paths(base_path, date, depth)
rad = 6371  # радиус Земли


def task_1():
    
    # 3 директории по каждому типу
    paths = input_event_paths(base_path, date, depth)

    # message type
    df_m = (
        spark
        .read
        .parquet(*[s for s in paths if "message" in s])
        .select("event.datetime","event.message_from","event.message_id","event.message_ts","lat","lon")
            )

    df_message = (
        df_m
        .select("datetime", "message_from", "message_id", "message_ts", "lat", "lon")
        .withColumn('timestamp', F.coalesce(df_m['datetime'],df_m['message_ts']))
        .drop('datetime','message_ts')
        .withColumn('ts', F.to_timestamp('timestamp'))
        .drop('timestamp')
        .withColumn('dt', F.to_date("ts"))         
            )

    # reaction type
    df_r = (
        spark
        .read
        .parquet(*[s for s in paths if "reaction" in s])
        )

    df_reaction = (
        df_r
        .select("event.datetime", "event.reaction_from", "event.reaction_type", "lat", "lon")
        .where('lat is not null and lon is not null')
        )

    # subscription type
    df_s = (
        spark
        .read
        .parquet(*[s for s in paths if "subscription" in s])
        )
    df_subscription = (
        df_s
        .select('event.datetime', 'event.subscription_channel', 'event.user', 'lat', 'lon')
        )

    
    
    # кросс джойн на города, чтобы высчитать мин расстояния
    df_cross_m = (df_message.crossJoin(city.selectExpr("id","city", "tz", "cast(lat_city as double) lat_city" , "cast(lng as double) lng")))
    

    df_cross_m_city = (
        df_cross_m
        .withColumn('distance', 2 * rad * F.asin( 
                                                    (
                                                        ( F.sin(F.col('lat_city')/2 - F.col('lat')/2 ) )**2 +
                                                        F.cos( F.col('lat_city') ) * F.cos( F.col('lat') ) *
                                                        ( F.sin( F.col('lng')/2 - F.col('lon')/2 ) )**2
                                                    )**0.5

                                                )
                    )
        .withColumn('rnk', F.row_number().over(Window().partitionBy(['message_id']).orderBy(F.asc('distance'))))
        .where("rnk = 1")

            )

    df_cross_r = (df_reaction.crossJoin(city.selectExpr("id","city", "tz", "cast(lat_city as double) lat_city" , "cast(lng as double) lng")))

    df_cross_r_city = (
        df_cross_r
        .withColumn('distance', 2 * rad * F.asin( 
                                                            (
                                                                ( F.sin(F.col('lat_city')/2 - F.col('lat')/2 ) )**2 +
                                                                F.cos( F.col('lat_city') ) * F.cos( F.col('lat') ) *
                                                                ( F.sin( F.col('lng')/2 - F.col('lon')/2 ) )**2
                                                            )**0.5

                                                        )
                            )
        .withColumn('rnk', F.row_number().over(Window().partitionBy(['reaction_from', 'datetime']).orderBy(F.asc('distance'))))
        .where('rnk = 1')
        .withColumn('month', F.month("datetime"))
        .withColumn('week', F.weekofyear('datetime'))
        .withColumnRenamed('datetime', 'ts')


            )
    
    df_cross_s = (df_subscription.crossJoin(city.selectExpr("id","city", "tz", "cast(lat_city as double) lat_city" , "cast(lng as double) lng")))

    df_cross_s_city = (
        df_cross_s
        .withColumn('distance', 2 * rad * F.asin( 
                                                    (
                                                        ( F.sin(F.col('lat_city')/2 - F.col('lat')/2 ) )**2 +
                                                        F.cos( F.col('lat_city') ) * F.cos( F.col('lat') ) *
                                                        ( F.sin( F.col('lng')/2 - F.col('lon')/2 ) )**2
                                                    )**0.5

                                                    )
                    )
        .withColumn('rnk', F.row_number().over(Window().partitionBy(['subscription_channel', 'user', 'datetime']).orderBy(F.asc('distance'))))
        .where('rnk = 1')
        .withColumn('month', F.month("datetime"))
        .withColumn('week', F.weekofyear('datetime'))
        .withColumnRenamed('datetime', 'ts')
            )
    
    
    

    events = (
        df_cross_m_city
        .select('message_from', 'id','city', 'ts', 'tz')
        .union(df_cross_r_city.select('reaction_from', 'id','city','ts', 'tz'))
        .union(df_cross_s_city.select('user', 'id', 'city', 'ts', 'tz'))
        .withColumn('dt', F.to_date("ts"))
    )
    
    
    latest_event = (
        events
        .withColumn('rnk', F.row_number().over(Window().partitionBy(['message_from']).orderBy(F.desc('ts'))))
        .where('rnk = 1')
        .select(F.col('message_from').alias('user_id'), F.col('city').alias('act_city'), F.col('ts'), F.col('tz'))
    )
        
        
    home_city_calc = (
        events
        .select('message_from', 'dt','id', 'city')
        .distinct()
        .groupBy("message_from")
        .agg(F.count("id").alias('travel_count'))
    )
    
    home_city_calc_list = (
        events
        .select('message_from', 'dt','id', 'city')
        .distinct()
        .groupBy("message_from")
        .agg(F.collect_list('city').alias('travel_array'))
    )
    
    home_city = (
        events
        .withColumn('prev_city', F.lag("city", 1).over(Window().partitionBy('message_from').orderBy('ts')))
        .withColumn('flag',F.expr("CASE WHEN prev_city != city or prev_city is null THEN 1 ELSE 0 END"))
        .withColumn('cumsum', F.sum('flag').over(Window.partitionBy(['message_from','city'])
                                                 .orderBy('ts')
                                                 .rangeBetween(Window.unboundedPreceding, 0)))
        .withColumn('min_dt', F.min('dt').over(Window.partitionBy(['message_from','id','cumsum'])))
        .withColumn('max_dt', F.max('dt').over(Window.partitionBy(['message_from','id','cumsum'])))
        .withColumn('datediff', F.datediff(F.col('max_dt'), F.col("min_dt")))
        .where('datediff >= 2') # пока ставим 2 чтоб считалось быстро - по заданию нужно 27
        .withColumn('rnk', F.row_number().over(Window().partitionBy(['message_from']).orderBy(F.desc('ts'))))
        .where('rnk = 1')
        .select(F.col('message_from'), F.col('city').alias('home_city') )
    )
    
    
    
    final_task_1 = (
                latest_event
                .join(home_city, home_city.message_from == latest_event.user_id, how = 'left')
                .where('user_id = message_from')
                .drop('message_from')
                .join(home_city_calc, home_city_calc.message_from == latest_event.user_id, how = 'left')
                .where('user_id = message_from')
                .drop('message_from')
                .join(home_city_calc_list, home_city_calc_list.message_from == latest_event.user_id, how = 'left')
                .where('user_id = message_from')
                .drop('message_from')
                .withColumn("local_time", F.from_utc_timestamp(F.col("ts"), F.col('tz')))
                .select(F.col('user_id'), F.col('act_city'), F.col('home_city'), F.col('travel_count'), 
                        F.col('travel_array') , F.col('local_time'))
    )

    return final_task_1





#########################################################################################
# 2 витрина

def task_2():

    paths = input_event_paths(base_path, date, depth)

    # message type
    df_m = (
        spark
        .read
        .parquet(*[s for s in paths if "message" in s])
        #.where('event.message_from=1390')
        .select("event.datetime","event.message_from","event.message_id","event.message_ts","lat","lon")
            )

    df_message = (
        df_m
        .select("datetime", "message_from", "message_id", "message_ts", "lat", "lon")
        .withColumn('timestamp', F.coalesce(df_m['datetime'],df_m['message_ts']))
        .drop('datetime','message_ts')
        .withColumn('ts', F.to_timestamp('timestamp'))
        .drop('timestamp')
        .withColumn('dt', F.to_date("ts"))         
            )

    # reaction type
    df_r = (
        spark
        .read
        .parquet(*[s for s in paths if "reaction" in s])
        #.where('event.reaction_from=1390')
        )

    df_reaction = (
        df_r
        .select("event.datetime", "event.reaction_from", "event.reaction_type", "lat", "lon")
        .where('lat is not null and lon is not null')
        )

    # subscription type
    df_s = (
        spark
        .read
        .parquet(*[s for s in paths if "subscription" in s])
        #.where('event.user=1390')
        )
    df_subscription = (
        df_s
        .select('event.datetime', 'event.subscription_channel', 'event.user', 'lat', 'lon')
        )


    # кросс джойн на города, чтобы высчитать мин расстояния
    df_cross_m = (df_message.crossJoin(city.selectExpr("id","city", "tz", "cast(lat_city as double) lat_city" , "cast(lng as double) lng")))


    df_cross_m_city = (
        df_cross_m
        .withColumn('distance', 2 * rad * F.asin( 
                                                    (
                                                        ( F.sin(F.col('lat_city')/2 - F.col('lat')/2 ) )**2 +
                                                        F.cos( F.col('lat_city') ) * F.cos( F.col('lat') ) *
                                                        ( F.sin( F.col('lng')/2 - F.col('lon')/2 ) )**2
                                                    )**0.5

                                                )
                    )
        .withColumn('rnk', F.row_number().over(Window().partitionBy(['message_id']).orderBy(F.asc('distance'))))
        .where("rnk = 1")
        .withColumn("month",F.trunc(F.col("ts"), "month"))
        .withColumn("week",F.trunc(F.col("ts"), "week"))

            )

    df_cross_r = (df_reaction.crossJoin(city.selectExpr("id","city", "tz", "cast(lat_city as double) lat_city" , "cast(lng as double) lng")))

    df_cross_r_city = (
        df_cross_r
        .withColumn('distance', 2 * rad * F.asin( 
                                                    (
                                                        ( F.sin(F.col('lat_city')/2 - F.col('lat')/2 ) )**2 +
                                                        F.cos( F.col('lat_city') ) * F.cos( F.col('lat') ) *
                                                        ( F.sin( F.col('lng')/2 - F.col('lon')/2 ) )**2
                                                    )**0.5

                                                )
                            )
        .withColumn('rnk', F.row_number().over(Window().partitionBy(['reaction_from', 'datetime']).orderBy(F.asc('distance'))))
        .where('rnk = 1')
        .withColumn("month",F.trunc(F.col("datetime"), "month"))
        .withColumn("week",F.trunc(F.col("datetime"), "week"))
        .withColumnRenamed('datetime', 'ts')


            )

    df_cross_s = (df_subscription.crossJoin(city.selectExpr("id","city", "tz", "cast(lat_city as double) lat_city" , "cast(lng as double) lng")))

    df_cross_s_city = (
        df_cross_s
        .withColumn('distance', 2 * rad * F.asin( 
                                                    (
                                                        ( F.sin(F.col('lat_city')/2 - F.col('lat')/2 ) )**2 +
                                                        F.cos( F.col('lat_city') ) * F.cos( F.col('lat') ) *
                                                        ( F.sin( F.col('lng')/2 - F.col('lon')/2 ) )**2
                                                    )**0.5

                                                    )
                    )
        .withColumn('rnk', F.row_number().over(Window().partitionBy(['subscription_channel', 'user', 'datetime']).orderBy(F.asc('distance'))))
        .where('rnk = 1')
        .withColumn("month",F.trunc(F.col("datetime"), "month"))
        .withColumn("week",F.trunc(F.col("datetime"), "week"))
        .withColumnRenamed('datetime', 'ts')
            )


    base = (
        df_cross_m_city
        .select('month', 'week','id')
        .union(df_cross_r_city.select('month', 'week','id'))
        .union(df_cross_s_city.select('month', 'week','id'))
        .distinct()
        )


    week_message = (   
        df_cross_m_city
        .groupby(['month', 'week', 'id'])
        .agg(F.count('message_id').alias('week_message'))
        )


    month_message = (   
        df_cross_m_city
        .groupby(['month', 'id'])
        .agg(F.count('message_id').alias('month_message'))
        )

    week_user = (   
        df_cross_m_city
        .select('message_from', 'message_id', 'ts', 'id', 'month', 'week')\
        .withColumn("first_ts", F.min(F.col("ts")).over(Window.partitionBy("message_from")))\
        .withColumn('new_user', F.expr("CASE WHEN first_ts = ts then 1 else 0 end" ))\
        .groupby(['month', 'week', 'id'])
        .agg(F.sum('new_user').alias('week_user'))
        )

    month_user = (   
        df_cross_m_city
        .select('message_from', 'message_id', 'ts', 'id', 'month', 'week')\
        .withColumn("first_ts", F.min(F.col("ts")).over(Window.partitionBy("message_from")))\
        .withColumn('new_user', F.expr("CASE WHEN first_ts = ts then 1 else 0 end" ))\
        .groupby(['month', 'id'])
        .agg(F.sum('new_user').alias('month_user'))
        )

    week_reaction = (
        df_cross_r_city
        .groupby(['month', 'week', 'id'])
        .agg(F.count('reaction_from').alias('week_reaction'))
        )


    month_reaction = (
        df_cross_r_city
        .groupby(['month', 'id'])
        .agg(F.count('reaction_from').alias('month_reaction'))
        )


    week_subscription = (
        df_cross_s_city
        .groupby(['month', 'week', 'id'])
        .agg(F.count('user').alias('week_subscription'))
        )


    month_subscription = (
        df_cross_s_city
        .groupby(['month', 'id'])
        .agg(F.count('user').alias('month_subscription'))
        )

    task_2 = (
        base
        .join(week_message, ['month','week', 'id'], how='left')
        .drop('week_message.month', 'week_message.week', 'week_message.id')

        .join(month_message, ['month','id'], how='left')
        .drop('month_message.month', 'month_message.id')

        .join(week_user, ['month','week','id'], how='left')
        .drop('week_user.month', 'week_user.id', 'week_user.week')

        .join(month_user, ['month','id'], how='left')
        .drop('month_user.month', 'month_user.id')

        .join(week_reaction, ['month','week','id'], how='left')
        .drop('week_reaction.month', 'week_reaction.id', 'week_reaction.week')

        .join(month_reaction, ['month','id'], how='left')
        .drop('month_reaction.month', 'month_reaction.id')

        .join(week_subscription, ['month','week','id'], how='left')
        .drop('week_subscription.month', 'week_subscription.id', 'week_subscription.week')

        .join(month_subscription, ['month','id'], how='left')
        .drop('month_subscription.month', 'month_subscription.id')
        
        .select('month','week',F.col('id').alias('zone_id'),'week_message','week_reaction','week_subscription',
               'week_user', 'month_message', 'month_reaction', 'month_subscription','month_user')
    )
    
    return task_2


#########################################################################################
# 3 витрина

def task_3():
    
    # 3 директории по каждому типу
    paths = input_event_paths(base_path, date, depth)

    # message type
    df_m = (
        spark
        .read
        .parquet(*[s for s in paths if "message" in s])
        .select("event.datetime","event.message_from","event.message_to","event.message_id","event.message_ts","lat","lon")
            )

    df_message = (
        df_m
        .select("datetime", "message_from", "message_id", "message_to", "message_ts", "lat", "lon")
        .withColumn('timestamp', F.coalesce(df_m['datetime'],df_m['message_ts']))
        .drop('datetime','message_ts')
        .withColumn('ts', F.to_timestamp('timestamp'))
        .drop('timestamp')
        .withColumn('dt', F.to_date("ts"))         
            )

    # reaction type
    df_r = (
        spark
        .read
        .parquet(*[s for s in paths if "reaction" in s])
        )

    df_reaction = (
        df_r
        .select("event.datetime", "event.reaction_from", "event.reaction_type", "lat", "lon")
        .where('lat is not null and lon is not null')
        )

    # subscription type
    df_s = (
        spark
        .read
        .parquet(*[s for s in paths if "subscription" in s])
        )
    df_subscription = (
        df_s
        .select('event.datetime', 'event.subscription_channel', 'event.user', 'lat', 'lon')
        )

    
    
    # кросс джойн на города, чтобы высчитать мин расстояния
    df_cross_m = (df_message.crossJoin(city.selectExpr("id","city", "tz", "cast(lat_city as double) lat_city" , "cast(lng as double) lng")))
    

    df_cross_m_city = (
        df_cross_m
        .withColumn('distance', 2 * rad * F.asin( 
                                                    (
                                                        ( F.sin(F.col('lat_city')/2 - F.col('lat')/2 ) )**2 +
                                                        F.cos( F.col('lat_city') ) * F.cos( F.col('lat') ) *
                                                        ( F.sin( F.col('lng')/2 - F.col('lon')/2 ) )**2
                                                    )**0.5

                                                )
                    )
        .withColumn('rnk', F.row_number().over(Window().partitionBy(['message_id']).orderBy(F.asc('distance'))))
        .where("rnk = 1")

            )

    df_cross_r = (df_reaction.crossJoin(city.selectExpr("id","city", "tz", "cast(lat_city as double) lat_city" , "cast(lng as double) lng")))

    df_cross_r_city = (
        df_cross_r
        .withColumn('distance', 2 * rad * F.asin( 
                                                            (
                                                                ( F.sin(F.col('lat_city')/2 - F.col('lat')/2 ) )**2 +
                                                                F.cos( F.col('lat_city') ) * F.cos( F.col('lat') ) *
                                                                ( F.sin( F.col('lng')/2 - F.col('lon')/2 ) )**2
                                                            )**0.5

                                                        )
                            )
        .withColumn('rnk', F.row_number().over(Window().partitionBy(['reaction_from', 'datetime']).orderBy(F.asc('distance'))))
        .where('rnk = 1')
        .withColumn('month', F.month("datetime"))
        .withColumn('week', F.weekofyear('datetime'))
        .withColumnRenamed('datetime', 'ts')


            )
    
    df_cross_s = (df_subscription.crossJoin(city.selectExpr("id","city", "tz", "cast(lat_city as double) lat_city" , "cast(lng as double) lng")))

    df_cross_s_city = (
        df_cross_s
        .withColumn('distance', 2 * rad * F.asin( 
                                                    (
                                                        ( F.sin(F.col('lat_city')/2 - F.col('lat')/2 ) )**2 +
                                                        F.cos( F.col('lat_city') ) * F.cos( F.col('lat') ) *
                                                        ( F.sin( F.col('lng')/2 - F.col('lon')/2 ) )**2
                                                    )**0.5

                                                    )
                    )
        .withColumn('rnk', F.row_number().over(Window().partitionBy(['subscription_channel', 'user', 'datetime']).orderBy(F.asc('distance'))))
        .where('rnk = 1')
        .withColumn('month', F.month("datetime"))
        .withColumn('week', F.weekofyear('datetime'))
        .withColumnRenamed('datetime', 'ts')
            )
    
    
    
    events = (
        df_cross_m_city
        .select('message_from', 'id','city', 'ts', 'tz')
        .union(df_cross_r_city.select('reaction_from', 'id','city','ts', 'tz'))
        .union(df_cross_s_city.select('user', 'id', 'city', 'ts', 'tz'))
        .withColumn('dt', F.to_date("ts"))
    )
    
    # последенее событие на каждого user-id
    latest_event = (
        events
        .withColumn('rnk', F.row_number().over(Window().partitionBy(['message_from']).orderBy(F.desc('ts'))))
        .where('rnk = 1')
        .select(F.col('message_from').alias('user_id'), F.col('id'), F.col('city').alias('act_city'), F.col('ts'), F.col('tz'))
    )

    # находим user-id, которые подписаны на одинаковые каналы
    subchannels = (
        df_cross_s_city
        .withColumnRenamed('user', 'user_left')
        .select('subscription_channel', 'user_left')
        .join(df_cross_s_city.withColumnRenamed('user', 'user_right').select('subscription_channel', 'user_right'), on = ["subscription_channel"], how="inner")
        .drop('subscription_channel')
        .where('user_left != user_right')
    )

    # оставляем только уникальные комбинации, включая зеркальность
    cols = ['user_left', 'user_right']
    subchannels_unique = (
        subchannels
        .withColumn('arr', F.array_sort(F.array(*cols)))
        .drop_duplicates(['arr'])
        .drop('arr')#.orderBy('user_left', 'user_right')
    )

    # находим, тех, кто списывался друг с другом
    cols = ['message_from', 'message_to']
    friends = (
        df_cross_m_city
        .select('message_from', 'message_to')
        .withColumn('arr', F.array_sort(F.array(*cols)))
        .drop_duplicates(['arr'])
        .drop('arr')
        .withColumnRenamed('message_from', 'user_left')
        .withColumnRenamed('message_to', 'user_right')
    )

    not_friends = (
        subchannels_unique
        .join(friends, [friends.user_l == subchannels_unique.user_left, friends.user_r == subchannels_unique.user_right], how='leftanti')
        .join(friends, [friends.user_r == subchannels_unique.user_left, friends.user_l == subchannels_unique.user_right], how='leftanti')
    )

    not_friends_city = (
        not_friends
        .join(latest_event.withColumnRenamed('id', 'zone_id').select('user_id', 'zone_id'), 
        on=[latest_event.user_id==not_friends.user_left], how='left')
        
        .join(latest_event.withColumnRenamed('user_id', 'user').select('user', 'id', 'tz'), 
        on=[latest_event.user_id==not_friends.user_right], how='left')
    )

    task_3 = (
        not_friends_city
        .where('zone_id = id')
        .withColumn("processed_dttm", F.current_timestamp())
        .withColumn("local_time", F.from_utc_timestamp(F.col("processed_dttm"), F.col('tz')))
        .select('user_left', 'user_right','processed_dttm', 'zone_id', 'local_time')
    )

    return task_3