def DF_local_time(df_city: pyspark.sql.DataFrame, spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
        
    window_week = Window().partitionBy('week')
    window_month = Window().partitionBy('month')
    window = Window().partitionBy('event.message_from').orderBy(F.col('date'))

    df_activity = df_city \
        .withColumn("month",F.trunc(F.col("date"), "month"))\
        .withColumn("week",F.trunc(F.col("date"), "week"))\
        .withColumn("rn",F.row_number().over(window))\
        .withColumn("week_message",F.sum(F.when(df_city.event_type == "message",1).otherwise(0)).over(window_week))\
        .withColumn("week_reaction",F.sum(F.when(df_city.event_type == "reaction",1).otherwise(0)).over(window_week))\
        .withColumn("week_subscription",F.sum(F.when(df_city.event_type == "subscription",1).otherwise(0)).over(window_week))\
        .withColumn("week_user",F.sum(F.when(F.col('rn') == 1,1).otherwise(0)).over(window_week))\
        .withColumn("month_message",F.sum(F.when(df_city.event_type == "message",1).otherwise(0)).over(window_month)) \
        .withColumn("month_reaction",F.sum(F.when(df_city.event_type == "reaction",1).otherwise(0)).over(window_month)) \
        .withColumn("month_subscription",F.sum(F.when(df_city.event_type == "subscription",1).otherwise(0)).over(window_month))\
        .withColumn("month_user",F.sum(F.when(F.col('rn') == 1,1).otherwise(0)).over(window_month))\
        .drop("rn")
    return df_activity

window = Window().partitionBy('user').orderBy('date')
df_city_from = df_city.selectExpr('event.message_from as user','lat_double_fin', 'lng_double_fin', 'date')
df_city_to = df_city.selectExpr('event.message_to as user','lat_double_fin', 'lng_double_fin', 'date')
df = df_city_from.union(df_city_to)\
.select(F.col('user'), F.col('date'),F.last(F.col('lat_double_fin'),ignorenulls = True).over(window).alias('lat_to'),F.last(F.col('lng_double_fin'),ignorenulls = True).over(window).alias('lng_to') )

def df_friends(df_city: pyspark.sql.DataFrame, df:pyspark.sql.DataFrame,  spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
      
    window = Window().partitionBy('event.message_from', 'week', 'event.message_to')
    window_rn = Window().partitionBy('event.message_from').orderBy(F.col('date').desc())
    window_friend = Window().partitionBy('event.message_from')

    df_friends = df_city \
    .withColumn("week",F.trunc(F.col("date"), "week"))\
    .withColumn("cnt", F.count('*').over(window)) \
    .withColumn("rn",F.row_number().over(window_rn)) \
    .filter(F.col('rn')<=5) \
    .join(df, (df.user==df_city.event.message_to) & (df.date==df_city.date), 'left')\
    .withColumn('dif', F.acos(F.sin(F.col('lat_double_fin'))*F.sin(F.col('lat_to')) + F.cos(F.col('lat_double_fin'))*F.cos(F.col('lat_to'))*F.cos(F.col('lng_double_fin')-F.col('lng_to')))*F.lit(6371) )\
    .filter(F.col('dif')<=1) \
    .withColumn('friends', F.collect_list('event.message_to').over(window_friend) )\
    .join(DF_local_time, 'city', 'inner') \
    .selectExpr('event.message_from as user', 'friends', 'local_time' , 'TIME as time_UTC', 'week', 'city' )

