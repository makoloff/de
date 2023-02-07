# вывод метаданных кластера через kafka в докере
docker run -it --network=host -v "/home/yc-user/lessons/CA.pem:/data/CA.pem" edenhill/kcat:1.7.1 -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091  -X security.protocol=SASL_SSL  -X sasl.mechanisms=SCRAM-SHA-512  -X sasl.username="de-student"  -X sasl.password="ltcneltyn"  -X ssl.ca.location=/data/CA.pem -L

# запись -P
docker run -it --network=host -v "/home/yc-user/lessons:/data/lessons" edenhill/kcat:1.7.1 -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512 -X sasl.username="de-student" -X sasl.password="ltcneltyn" -X ssl.ca.location=/data/lessons/CA.pem -t base -K: -T -P -l "/data/lessons/old-yet-gold.txt"

# Отправка сообщения
kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username="de-student" \
-X sasl.password="ltcneltyn" \
-X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt \
-t base \
-K: \
-P
my_key:my_message

# чтение - C
docker run -it --network=host -v "/home/yc-user/lessons/CA.pem:/data/CA.pem" edenhill/kcat:1.7.1 -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512 -X sasl.username="de-student" -X sasl.password="ltcneltyn" -X ssl.ca.location=/data/CA.pem -t student.topic.cohort3.yc-user -C

# чтение - C
kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username="de-student" \
-X sasl.password="ltcneltyn" \
-X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt \
-t base \
-C \
-o beginning




'''
Режимы соотносятся с ключевыми группами действий, которые можно сделать с помощью kcat:
-P — режим продюсера, записи данных. Поможет самостоятельно записать одно или несколько сообщений, 
но не реализовать поток данных.
-C — режим консьюмера, чтения данных. Поможет как проверить, отправилось ли ваше кастомное сообщение, 
так и проверить, идёт ли настроенный поток данных.
-L — уже знакомый режим получения метаинформации.
-Q — режим запроса для поиска специфического фрагмента данных.
'''

'''
Код практически такой же, как для запроса метаинформации, но с несколькими новыми параметрами:
-t с названием топика, откуда нужно прочитать сообщение. В нашем случае это топик base.
-С (uppercase) переводит клиент kcat в режим консьюмера, то есть в режим чтения сообщений.
-o указывает, с какого офсета нужно начать читать, — проще говоря, с какого конца. 
По умолчанию вы начнёте читать с начала топика — самые новые — и сразу из всех партиций топика. 
Чтобы увидеть сначала самые старые сообщения, вы можете указать значение beginning и, соответственно, end, 
если с конца — то самые новые. Также вы можете начать читать с конкретной партиции. 
Для этого укажите его номер. В нашем случае указан beginning, а это значит, что мы начнём читать топик с самого начала, 
то есть с самых старых сообщений.
-с (lowercase) поможет указать, сколько сообщений вывести. В этом параметр похож на LIMIT в SQL.
'''

'''
SparkContext- можно сказать, что SparkContext соединяется с Cluster Manager и передаёт ему все параметры приложения.
'''

'''
SparkSession
Сессия позволяет подключиться к кластеру Spark, 
однако предоставляет API для работы с более высокоуровневыми структурами хранения данных. 
Взаимодействуя со Spark Structured Streaming, вы тоже будете использовать SparkSession.
'''

# 1 step
'''
Для начала подключите точку входа в Spark Streaming — SparkSession. 
Первый шаг ничем не отличается от того, что нужно сделать для батчевой обработки данных. 
А значит, можно легко трансформировать код батчевой обработки в код потоковой.
'''
from pyspark.sql import SparkSession

# 2 step
'''
Далее создайте SparkSession. Этот этап вам тоже знаком.
'''
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("SparkStreamingApplication") \
    .getOrCreate()

# 3 step
'''
Теперь внимание: нужно прочитать данные и создать датафрейм в потоковом режиме. 
Код для этого шага получится таким:
'''
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("SparkStreamingApplication") \
    .getOrCreate()

inputStreamDF = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

'''
Появляются отличия от батчевого режима.
1. Вместо метода read вызывается метод readStream. 
Тут и меняется концепция обработки данных. 
Пока что изменения, превращающие батчевую обработку в потоковую с помощью PySpark, заключаются только в одной строке.
2. Далее указывается тип источника.
 a) В вашем случае это format("socket") — socket-соединение по хосту localhost и порту 9999. 
Они передаются как дополнительные параметры к типу источника и могут меняться.
 b) Для типа источника Kafka — format("kafka") — указываются специальные параметры для Kafka. 
Например, такие как bootstrap.servers.
 c) Тип File Source — format("text") — для указания текстового формата файла. 
Кроме текстового, можно ещё указать csv и json.
Здесь важно вызвать метод `load()`, так как именно он говорит, 
что данные из источника можно передавать в новый датафрейм `inputStreamDF`.
'''

'''
Предположим, к вам поступают данные о пользователях. 
В них содержатся имя пользователя username, возраст age и сообщение от пользователя message. 
Чтобы потренироваться, преобразуйте входной датафрейм в новый, получив только сообщения от пользователей до 30 лет.
'''
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("SparkStreamingApplication") \
    .getOrCreate()

inputStreamDF = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

resultDF = inputStreamDF.select("message").where("age < 30")

'''
Осталось только отправить данные в сток. Например, на консоль.
'''
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("SparkStreamingApplication") \
    .getOrCreate()

inputStreamDF = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

resultDF = inputStreamDF.select("message").where("age < 30")

resultDF \
    .writeStream \
    .format("console") \
    .start()

'''
В отличие от батчевого режима, появляется метод writeStream. Именно writeStream указывает, 
что датафрейм resultDF — стриминговый, и с ним нужно работать в стриминговом режиме. 
Далее указывается тип стока — в вашем случае консоль console — и метод start(), 
который запустит всю цепочку потоковой обработки данных.
Итак, в создании Spark Streaming Application — почти всё то же самое, что и в батчевом режиме. 
Это очень удобно, если вы уже знакомы с батчевой обработкой данных в PySpark.
'''


# пример задачи
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, StringType

#создаём SparkSession
spark = SparkSession.builder \
    .appName("count words") \
    .getOrCreate()

#определяем схему для датафрейма
userSchema = StructType([StructField("text", StringType(), True)])

#читаем текст из файла
wordsDF = spark.readStream.schema(userSchema).format('text').load('/datas8')

#разделяем слова по запятым
splitWordsDF = wordsDF.select(explode(split(wordsDF.text, ",")).alias("word"))

#группируем слова
wordCountsDF = splitWordsDF.groupBy("word").count()

# запускаем стриминг
wordCountsDF.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start() \
    .awaitTermination()




# Код применения оконной функции для этого примера будет выглядеть так:
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("SparkStreamingApplicationWindowFunction") \
    .getOrCreate()

symbolsDF = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# streaming DataFrame of schema { timestamp: Timestamp, symbol: String }

windowedCounts = symbolsDF.groupBy( \
    window(symbolsDF.timestamp, "5 minutes", "2 minutes"),\
    symbolsDF.symbol \
).count()
'''
Для того чтобы воспользоваться скользящим окном, нужно:
В агрегирующей функции groupBy вызвать метод window с колонкой, которая содержит временную метку timestamp.
Передать timestamp размер скользящего окна 5 minutes и шаг по времени 2 minutes.
Указать колонку для агрегации symbol, куда будет записываться результат, и действие, 
которое нужно произвести с группированными данными, — count().
'''

# join в Spark Streaming
'''
Stream-Static/Static-Stream 
(пер. с англ. «Стриминговый-Статичный/Статичный-Стриминговый») — объединение данных из стриминга и статичной таблицы.
'''
from pyspark.sql import SparkSession

fromStaticTableDF = spark
                    .read \
                    .format('jdbc') \
                    .option('url', 'url') \
                    .option('driver', 'driverName') \
                    .option('dbtable', 'tableName') \
                    .option('user', 'username') \
                    .option('password', 'der_password') \
                    .load()

streamingDF = spark \
                        .readStream \
                        .format("socket") \
                        .option("host", "localhost") \
                        .option("port", 9999) \
                        .load()

streamingDf.join(fromStaticTableDF, "type")

'''
Этот режим пригодится, например, если вы обрабатываете геопозицию пользователя мобильного приложения в стриминге. 
Приложение отправляет только координаты местонахождения, но в вашу задачу входит определить, 
в каком городе находится пользователь. Одна таблица в БД содержит справочник из двух атрибутов: 
«Город», «Набор координат города». Если сделать join по координатам, то можно получить название города, 
в котором находится пользователь, — потоковые данные объединяются с полезной статичной информацией.
'''


'''
Stream-Stream 
(пер. с англ. «Стриминговый-Стриминговый») — объединение данных между двумя потоками стриминга. 
Каждый поток вычитывает свой микробатч, в результате формируются два отдельных датафрейма. 
Затем для них, как и в первом режиме, нужно вызвать join
'''

'''
Однако у режима Stream-Stream есть одна слабость: данные одного потока могут отставать от данных другого, проще говоря,
данные будут приходить в один из микробатчей позже. 
Это значит, что сопоставить данные для join в единый момент времени будет нельзя. 
Чтобы избежать подобных ситуаций, Spark Structured Streaming буферизует прошлое состояние потока. 
Но это не гарантирует избавления от несостыковки данных, поэтому, выбирая режим, инженер данных должен понимать, 
насколько этот фактор важен для выполнения задачи.
'''
from pyspark.sql import SparkSession

streamingClicksDF = spark \
                        .readStream \
                        .format("socket") \
                        .option("host", "localhost") \
                        .option("port", 9998) \
                        .load()

streamingGeoDF = spark \
                        .readStream \
                        .format("socket") \
                        .option("host", "localhost") \
                        .option("port", 9999) \
                        .load()

streamingClicksDF.join(streamingGeoDF, "type")

'''
Stream-Stream можно использовать для такой задачи. Предположим, у вас есть приложение, показывающее актуальные скидки. 
Вы работаете с двумя потоками данных: по геопозиции пользователя и его кликам в интернете. 
Если вы объедините потоки и увидите, что по геолокации пользователь сейчас находится в торговом центре 
и ищет по ссылкам в интернете бытовые приборы, то сможете предложить ему тот магазин бытовых приборов в торговом центре, 
где есть скидки на товары, интересные пользователю.
'''

'''
К тому же из-за того, что потоковый датафрейм присоединяется к датафрейму батчевому, 
Spark Streaming поддерживает только несколько типов join для каждого режима:
Режим	        Тип
Stream-Stream:	Inner, Left-Outer, Right-Outer, Full-Outer, Left-Semi
Stream-Static:	Inner, Left-Outer, Left-Semi
Static-Stream:	Inner, Right-Outer
'''




# пример с чтением из postgres
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, StringType

#необходимая библиотека для интеграции Spark и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.postgresql:postgresql:42.4.0",
        ]
    )

#создаём SparkSession и передаём библиотеку для работы с PostgreSQL
spark = SparkSession.builder \
    .appName("join data") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

#вычитываем данные из таблицы
tableDF = spark.read \
                    .format('jdbc') \
                    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'words') \
                    .option('user', 'login') \
                    .option('password', 'password') \
                    .load()

#определяем схему для DataFrame
userSchema = StructType([StructField("text", StringType(), True)])

#читаем текст из файла
wordsDF = spark.readStream.schema(userSchema).format('text').load('/datas8')

#разделяем слова по запятым
splitWordsDF = wordsDF.select(explode(split(wordsDF.text, ",")).alias("word"))

#объединяем данные. Присоединяем данные из таблицы к данным из файла 
joinDF = splitWordsDF.join(tableDF, splitWordsDF.word == tableDF.words, 'left')

#проверяем каких слов нет в таблице, но есть в файле filter(...isNull())
#возвращаем только один столбец (select(...))
#убираем дубли (distinct())
filterDF = joinDF.filter(joinDF.id.isNull()).select(joinDF.word).distinct()

#запускаем стриминг
filterDF.writeStream \
    .format("console") \
    .start() \
    .awaitTermination()



# пример парсинга json из kafka и чтение через Spark DataFrame API
from pyspark.sql import SparkSession
from pyspark.sql import functions as f, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import from_json

# необходимая библиотека с идентификатором в maven
# вы можете использовать ее с помощью метода .config и опции "spark.jars.packages"
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        ]
    )

# настройки security для кафки
# вы можете использовать из с помощью метода .options(**kafka_security_options)
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

# схема приходящего json
schema = StructType([
    StructField("subscription_id",IntegerType(),True)
    ,StructField("name",StringType(),True)
    ,StructField("description",StringType(),True)
    ,StructField("price",DoubleType(),True)
    ,StructField("currency",StringType(),True)
  ])

def spark_init() -> SparkSession:

    return SparkSession.builder \
    .appName("kafka task 2") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

def load_df(spark: SparkSession) -> DataFrame:

    return (
    spark
    .read
    .format('kafka')
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
    .options(**kafka_security_options)
    # .option('kafka.security.protocol', 'SASL_SSL')
    # .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
    # .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";')
    # .option('kafka.ssl.truststore.location', 'truststore_location')
    # .option('kafka.ssl.truststore.password', 'truststore_pass')
    .option("subscribe", "persist_topic")
    .option("startingOffsets", "earliest")
    .load()
    )

def transform(df: DataFrame) -> DataFrame:

    df2 = df.withColumn('val', f.col('value').cast("STRING")).drop('value')\
    .select(f.col('key').cast("STRING"), f.col('val'), 'topic', 'partition', 'offset', 'timestamp', 'timestampType')

    df3=df2.withColumn("value", from_json(df2.val, schema))\
    .selectExpr('value.*', '*')

    return df3.drop('value').withColumnRenamed('val', 'value')


spark = spark_init()

source_df = load_df(spark)
df = transform(source_df)

df.printSchema()
df.show(truncate=False)
df.show()



# Дедупликация
'''
Почему появляются дубликаты
Для этого есть несколько причин независимо от того, работаете ли вы с батчевой или с потоковой обработкой:
1) многокомпонентная система, к которой относится большинство современных систем;
2) ненадёжность сети;
3) некачественные данные в источнике.
'''

# код для дедупликации:
df=(df.dropDuplicates('колонки определяющие уникальность')
      .withWatermark('колонка со временем', 'граница ватермарки'))

# подключение к Postgresql 
'''
Подключение к Postgres через JDBC
Java Database Connectivity (пер. с англ. «связь с базами данных») — это интерфейс прикладного программирования, иначе говоря API, для Java, который определяет, как клиент может получить доступ к базе данных. И кроме того, снабжает методами для запроса и обновления данных в БД. Подключение, как и в случае с Kafka, происходит в несколько шагов, но порядок действий немного отличается.
1. Установить библиотеку для подключения к Postgres.
2. Создать сессию Spark.
3. Получить данные.

Установка библиотеки для подключения к Postgres. 
Чтобы Spark смог подсоединиться к внешнему источнику, ему нужна библиотека. 
Для подключения к Postgres это JDBC, реализованный в библиотеке PostgreSQL JDBC Driver, которая находится в Maven Repository. 
Maven Repository хранит библиотеки, в том числе и нужную вам. 
Пакетный менеджер Spark скачивает основную библиотеку и все зависимые от неё в момент старта Spark-приложения.
Но даже после того, как вы нашли нужную библиотеку, в Maven Repository легко запутаться,
потому что версий библиотек очень много. Правда, на самом деле всё довольно просто. 
Вам достаточно ввести имя нужной библиотеки и открыть вкладку в Maven Repository, которая имеет вид:

<!-- https://mvnrepository.com/artifact/org.postgresql/postgresql -->
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.4.2</version>
</dependency>

После чего — сформировать имя пакета как [groupId]:[artifactId]:[version]. 
В вашем случае имя пакета и будет выглядеть таким образом, как указано выше. 
Значит, правильное имя можно указать всего одной строчкой org.postgresql:postgresql:42.4.2.
Создание Spark-сессии. Используйте аргумент --packages, чтобы запустить Spark в интерактивном режиме с расширением:

pyspark --packages "org.postgresql:postgresql:42.4.0"
При создании Spark-приложения эта зависимость будет передаваться в config:
'''
spark = (
        SparkSession.builder.appName(test_name)
        .config("spark.jars.packages", "org.postgresql:postgresql:42.4.0")
        .getOrCreate()
    )

'''
Чтение данных из Postgres. Теперь нужно осуществить подключение по JDBC. Основные параметры подключения:
url — с помощью которого представляется база данных с JDBC. Вид url — jdbc:postgresql://host:port/database, где:
host — хост сервера БД;
port — порт сервера БД;
database — имя базы данных.
driver — имя класса драйвера JDBC, используемого для подключения к url. В вашем случае имя драйвера — org.postgresql.Driver.
user — пользователь для аутентификации. У вас это будет student.
password — пароль пользователя. Введите de-student.
Полную документацию можно посмотреть по ссылке. Чтобы перейти к чтению данных, останется только добавить метод load()
'''


# пример чтения streaming + чтение static из Postgresql, crossJoin обоих df

from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import from_json

postgresql_settings = {
    'user': 'master',
    'password': 'de-master-password'
}

spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0"
        ]
    )

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-admin\" password=\"de-kafka-admin-2022\";',
}


schema = StructType([
    StructField("client_id",StringType(),True)
    ,StructField("timestamp",TimestampType(),True)
    ,StructField("lat",DoubleType(),True)
    ,StructField("lon",DoubleType(),True)
  ])

TOPIC_NAME = 'student.topic.cohort3.yc-user'



def spark_init(test_name) -> SparkSession:

    return (SparkSession.builder \
    .appName({test_name}) \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate())




def read_marketing(spark: SparkSession) -> DataFrame:
    
    host = 'rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net'
    port = 6432
    schema = 'public'
    dbtable = 'public.marketing_companies'
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




def read_client_stream(spark: SparkSession) -> DataFrame:
    df = (
        spark
        .readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
        .options(**kafka_security_options)
        .option("subscribe", TOPIC_NAME)
        #.option("startingOffsets", "earliest")
        .load()
        )
    
    df2 = df.withColumn('val', f.col('value').cast(StringType())).drop('value')
 
    df3 = df2.withColumn("value", from_json(df2.val, schema))\
    .selectExpr('value.*', 'offset')
 
    return df3.select('client_id', 'timestamp', 'lat', 'lon', 'offset').dropDuplicates()\
    .withWatermark('timestamp', '5 minute')


def join(user_df, marketing_df) -> DataFrame:
    
    R = 6371000
    df = (
        user_df
        .crossJoin(marketing_df)
        .select('client_id', 'lat', 'lon', f.col('id').alias('adv_campaign_id'),f.col('name').alias('adv_campaign_name'), 
        f.col('description').alias('adv_campaign_description'), f.col('start_time').alias('adv_campaign_start_time')
        ,f.col('end_time').alias('adv_campaign_end_time'), f.col('point_lat').alias('adv_campaign_point_lat'),
        f.col('point_lon').alias('adv_campaign_point_lon'), f.current_timestamp().alias('created_at'), 'offset'
        )
    )

    df2 = df.withColumn('distance', R * 2 * f.asin( 
                                                    (
                                                        ( f.sin(f.col('adv_campaign_point_lat')/2 - f.col('lat')/2 ) )**2 +
                                                        f.cos( f.col('adv_campaign_point_lat') ) * f.cos( f.col('lat') ) *
                                                        ( f.sin( f.col('adv_campaign_point_lon')/2 - f.col('lon')/2 ) )**2
                                                    )**0.5

                                                )
    )

    return df2



if __name__ == "__main__":
    spark = spark_init('join stream')
    client_stream = read_client_stream(spark)
    marketing_df = read_marketing(spark)
    result = join(client_stream, marketing_df)

    query = (result
             .writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", False)
             .start())
    query.awaitTermination()




'''
Для отправки сообщений из стриминг-сервиса вам потребуется объект DataStreamWriter. С его помощью можно задать настройки экспорта. Чтобы получить к нему доступ, вызовите у датафрейма свойство writeStream и задайте три настройки потоковой обработки:
таргет;
триггер — частоту, с которой Spark запрашивает данные у источника;
режим вывода — способ компоновки финального датафрейма.
В коде это будет выглядеть так:
'''
(
    df.writeStream  # DataStreamWriter вызвали через .writeStream к датафрейму df
    .outputMode("метод") # complete, update или append
    .format("output sink") # File Sink, Kafka Sink, Console Sink
    .trigger(триггер)
    .start()
)

'''
Если выбрать таргет File Sink, выходное сообщение можно сохранить как файл в форматах CSV, JSON, ORC и Parquet. 
Console Sink обычно используется, чтобы протестировать код для Spark Structured Streaming в сочетании с Socket source
Чтобы задать в качестве таргета file sink, нужно прописать нужный формат файла в параметре format и директорию в option:
'''
(
    df.writeStream
    .format("parquet") # can be "orc", "json", "csv", etc.
    .option("path", "path/to/destination/dir")
    .start()
)

'''
Если таргетом должен быть топик Kafka, укажите в параметре format значение kafka, 
в одной строке option — все необходимые настройки, а в другой – название топика. 
Вот так:
'''
(
    df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    .option("topic", f"{имя вашего топика}")
    .start()
)

'''
Выберите в качестве таргета консоль, указав в параметре format значение console:
'''
(
    df.writeStream
    .format("console")
    .start()
)

'''
Третья настройка потоковой обработки, которую нужно указать на последнем этапе, — триггер. 
Его задают через одноимённый параметр trigger
'''
(
    df.writeStream  
    .format("ваш output sink")
    .trigger(ваш триггер)
    .start()
)



# Примеры команд, с помощью которых можно указать триггеры разных типов:

# фиксированный интервал: ProcessingTime trigger with two-seconds micro-batch interval
(df.writeStream
  .trigger(processingTime='2 seconds'))

# одноразовая обработка:
# Available-now trigger
(df.writeStream
  .trigger(availableNow=True))

# непрерывная обработка
# Continuous trigger with one-second checkpointing interval
(df.writeStream
  .trigger(continuous='1 second'))

# одноразовая обработка с разделением на микробатчи:
# One-time trigger
(df.writeStream
  .trigger(once=True))

'''
CheckpointLocation
При настройке отправки данных из стриминг-сервиса важно прописать в коде опцию checkpointLocation. 
Она нужна для страховки процесса от последствий сбоев, которые часто происходят из-за обрывов сети. 
Благодаря этой опции система будет знать, какие данные она уже обработала, и в случае ошибки возобновит работу с нужной точки.
Вот как прописать checkpointLocation:
'''
(df.writeStream
  .option("checkpointLocation", "{путь к папке}"))

'''
Kafka принимает данные в сериализованном виде. 
Так происходит потому, что технически этот брокер может получать сообщения только с одним параметром — value, 
значение которого должно быть либо в формате string, либо binary. 
Но как быть, если нужно отправить целый ряд параметров?
Решение простое! Все поля нужно сложить внутрь value, например:
'''
{
    "value": {
        "client_id": идентификатор клиента
        "adv_campaign_id": идентификатор рекламной акции
        "adv_campaign_name": описание рекламной акции
        "adv_campaign_description": описание рекламной акции
        "adv_campaign_start_time": время начала акции
        "adv_campaign_end_time": время окончания акции
        "adv_campaign_point_lat": расположение ресторана/точки широта
        "adv_campaign_point_lon": расположение ресторана/долгота широта
        "created_at": время создания выходного ивента
        "offset": офсет оригинального сообщения из Kafka
     }
}

# Затем сериализовать value с помощью метода to_json()
(
    df
    .withColumn('value', f.to_json(
        f.struct(f.col('{колонка 1}'), f.col('{колонка 2}'))
        ))
)
#  Когда вы их указали, нужно запустить поток методом .start()



