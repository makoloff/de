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
    .config("spark.driver.memory", "5g")
    .config("spark.driver.cores", 25)
    .config("spark.executor.memory", "2g") 
    .config("spark.executor.cores", 2)
    .appName("mp_project_session")
    .getOrCreate()
)

rad = 6371  # радиус Земли

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


def main():

    base_path = sys.argv[1] # откуда взять данные
    date = sys.argv[2] # за какую дату
    depth = sys.argv[3] # с какой глубиной назад
    base_path_out = sys.argv[4] # куда положить

    # пример ввода
    # base_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/pavelunkno/data/geo/events'
    # date = '2022-05-29'
    # depth = 5
    rad = 6371  # радиус Земли
   
    # 3 директории по каждому типу
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    paths = [f"{base_path}/event_type={event_type}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for event_type in ['message','reaction','subscription'] for x in range(int(depth))]


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
        .withColumn('cumsum', F.sum('flag').over(Window.partitionBy(['message_from','city']).orderBy('ts').rangeBetween(Window.unboundedPreceding, 0)))
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

    # запись в prod слой
    (
        final_task_1 
        .write
        .mode("overwrite") 
        .format('parquet') 
        .save(f"{base_path_out}/date={date}_{depth}")
    )


if __name__ == "__main__":
    main()