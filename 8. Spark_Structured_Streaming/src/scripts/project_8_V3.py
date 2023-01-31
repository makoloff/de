from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import from_json
from time import sleep

# топики для входящих сообщений (акции ресторанов) и выходящих (для push-уведомления в телефон)
TOPIC_NAME_in = 'yc-user_in'
TOPIC_NAME_out = 'yc-user_out'

# библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0"
    ]
)

# креды для бд
postgresql_settings = {
    'user': 'master',
    'password': 'de-master-password'
}

# настройки security для kafka
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-admin\" password=\"de-kafka-admin-2022\";',
}

# схема входного сообщения
schema = StructType([
    StructField("restaraunt_id",StringType(),True)
    ,StructField("adv_campaign_id",StringType(),True)
    ,StructField("adv_campaign_content",StringType(),True)
    ,StructField("adv_campaign_owner",StringType(),True)
    ,StructField("adv_campaign_owner_contact",StringType(),True)
    ,StructField("adv_campaign_datetime_start",DoubleType(),True)
    ,StructField("adv_campaign_datetime_end",DoubleType(),True)
    ,StructField("datetime_created",DoubleType(),True)
  ])

# определяем текущее время в UTC в миллисекундах
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))


# создание сессии
def spark_init(test_name) -> SparkSession:

    return (
        SparkSession.builder
        .appName({test_name})
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", spark_jars_packages)
        .getOrCreate()
    )

# читаем из топика Kafka сообщения с акциями от ресторанов 
def read_promo_stream(spark: SparkSession) -> DataFrame:

    # определяем текущее время в UTC в миллисекундах
    current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

    df = (
        spark
        .readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4****.mdb.yandexcloud.net:9091')
        .options(**kafka_security_options)
        .option("subscribe", TOPIC_NAME_in)
        .option("startingOffsets", "earliest")
        .load()
        )
    
    df2 = df.withColumn('val', F.col('value').cast(StringType())).drop('value')
    df3 = (df2.withColumn("value", from_json(df2.val, schema)).selectExpr('value.*')\
        .withColumn('datetime_created_ts', F.from_unixtime('datetime_created').cast(TimestampType()))\
        .where(f" adv_campaign_datetime_start <= {current_timestamp_utc} ")\
        .where(f" adv_campaign_datetime_end > {current_timestamp_utc} ")
        )

    return (
        df3
        .select(F.col('restaraunt_id').alias('restaraunt_id'),'adv_campaign_id','adv_campaign_content','adv_campaign_owner',
        'adv_campaign_owner_contact','adv_campaign_datetime_start','adv_campaign_datetime_end','datetime_created','datetime_created_ts')
        .dropDuplicates()
        .withWatermark('datetime_created_ts', '5 minute')
    )


# чтение данных из postgresql
def read_pg_client(spark: SparkSession) -> DataFrame:

    host = 'rc1a-fswjkpli01za****.mdb.yandexcloud.net'
    port = 6432
    schema = 'public'
    dbtable = 'public.subscribers_restaurants'
    databasename = 'de'
    url = 'jdbc:postgresql://' + str(host) + ':' + str(port) + '/' + str(databasename)

    return  (
        spark
        .read
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", dbtable) \
        .options(**postgresql_settings)
        .option("driver", "org.postgresql.Driver") \
        .load()
    )


def join(promo_kafka_df, client_pg_df) -> DataFrame:

    df = (
        promo_kafka_df
        .join(client_pg_df.drop('id').distinct(), client_pg_df.restaraunt_id==promo_kafka_df.restaraunt_id, 'inner')
        .select(
            promo_kafka_df['restaraunt_id']#.alias('restaurant_id'), 
            ,promo_kafka_df['adv_campaign_id'], 
            promo_kafka_df['adv_campaign_content'], 
            promo_kafka_df['adv_campaign_owner'], 
            promo_kafka_df['adv_campaign_owner_contact'], 
            promo_kafka_df['adv_campaign_datetime_start'], 
            promo_kafka_df['adv_campaign_datetime_end'], 
            promo_kafka_df['datetime_created'], 
            client_pg_df['client_id'], 
            F.unix_timestamp(F.current_timestamp(),"MM-dd-yyyy HH:mm:ss").alias("trigger_datetime_created"))
    )

    return df

# отдельная функция для заливки в PG, как 2-й временный способ
def write_to_postgres(df, epoch_id):

    mode="append"
    host = 'rc1a-fswjkpli01za****.mdb.yandexcloud.net'
    port = 6432
    schema = 'public'
    dbtable = 'public.subscribers_feedback'
    databasename = 'de'
    url = 'jdbc:postgresql://' + str(host) + ':' + str(port) + '/' + str(databasename)
    postgresql_settings = {
    'user': 'master',
    'password': 'de-master-password',
    "driver": "org.postgresql.Driver"
    }
    df.write.jdbc(url=url, table=dbtable, mode=mode, properties=postgresql_settings)


# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):

    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df_persist = df.persist()

    # записываем df в PostgreSQL с полем feedback
    mode="append"
    host = 'rc1a-fswjkpli01za****.mdb.yandexcloud.net'
    port = 6432
    schema = 'public'
    dbtable = 'public.subscribers_feedback'
    databasename = 'de'
    url = 'jdbc:postgresql://' + str(host) + ':' + str(port) + '/' + str(databasename)
    postgresql_settings = {
    'user': 'master',
    'password': 'de-master-password',
    "driver": "org.postgresql.Driver"
    }

    print('start loading into PG ------------------>')
    # грузим в PG
    (
        df_persist
        .withColumn('feedback', F.lit(None).cast(StringType()))
        .select('restaraunt_id','adv_campaign_id','adv_campaign_content','adv_campaign_owner','adv_campaign_owner_contact',
        'adv_campaign_datetime_start','adv_campaign_datetime_end','datetime_created','client_id','trigger_datetime_created',
        'feedback')
        .write
        .jdbc(url=url, table=dbtable, mode=mode, properties=postgresql_settings)
    )    

    print('data was loaded in PG ------------------>')


    print('start loading into Topic_out ------------------>')
    # создаём df для отправки в Kafka. Сериализация в json.    
    join_df_to_kafka = (
        df_persist
        .withColumn('value', F.to_json(
            F.struct('restaraunt_id','adv_campaign_id','adv_campaign_content','adv_campaign_owner',
            'adv_campaign_owner_contact','adv_campaign_datetime_start','adv_campaign_datetime_end','client_id','datetime_created',
            'trigger_datetime_created'))
        )
        .select('value'))

    # отправляем сообщения в результирующий топик Kafka без поля feedback    
    query_to_kafka = (
        join_df_to_kafka
        .write
        .format("kafka")
        #.outputMode("append")
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4****.mdb.yandexcloud.net:9091')
        .options(**kafka_security_options)
        .option("topic", TOPIC_NAME_out)
        .option("checkpointLocation", "/tmp/folder_tmp/checkpoint")
        .option("truncate", False)
        .save()
        )

    # while query_to_kafka.isActive:
    #     print(f"query information: runId={query_to_kafka.runId}, "
    #           f"status is {query_to_kafka.status}, "
    #           f"recent progress={query_to_kafka.recentProgress}")
    #     sleep(30)

    # query_to_kafka.awaitTermination()

    print('data was loaded in Topic_out ---------------------->')

    # очищаем память от df
    df_persist.unpersist()
    print('df was deleted from cache ---------------------->')



spark = spark_init('read stream project 8')
print('Session is started', '-------------------->')
# читаем из топика Kafka сообщения с акциями от ресторанов 
restaurant_read_stream_df = read_promo_stream(spark)
print('restaurant_read_stream_df is read', '-------------------->')
# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = read_pg_client(spark)
print('subscribers_restaurant_df is read', '-------------------->')

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = join(restaurant_read_stream_df, subscribers_restaurant_df)
print('result_df is read', '-------------------->')

# запускаем стриминг
print('запускаем стриминг -------------------->')
(
    result_df
    .writeStream \
    .trigger(processingTime="15 seconds")
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()
)
