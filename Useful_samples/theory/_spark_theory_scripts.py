# ссылка на google colab notebook с простыми примерами операций в PySpark
https://colab.research.google.com/drive/1sU1HG3_T9uGkHumLKOeSAPBxO2h6eJXh?usp=sharing



# Если возникнут проблемы с подключением из Jupyter Notebook к Spark, пропишите в начале работы в Jupyter следующее:
import findspark
findspark.init()
findspark.find()
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'


from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", 1) \
        .appName("Python Spark basic example") \   
        .getOrCreate()  


# установка на драйвере памяти и ядер
from pyspark.sql import SparkSession
spark = (
    SparkSession
    .builder
    .master("local[10]")
    .config("spark.driver.memory", "20g")
    .config("spark.driver.cores", 35)
    .appName("My session")
    .getOrCreate()
)

'''
Именно через SparkContext происходит взаимодействие с основной структурой данных Spark — RDD. 
Поэтому если для SparkContext будут заданы настройки по умолчанию, например, количество узлов или объем памяти, 
то ваши настройки для сессии не смогут переопределить их. 
В таком случае вам нужно поработать с настройками SparkContext, изменив настройки в SparkConf — объекта, 
содержащего в себе настройки параметров SparkContext. 
Они меняются с помощью set
'''

from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Job").set("spark.driver.cores", "2") # тут задаются все параметры SparkContext
sc = SparkContext(conf=conf) # инициализируется SparkContext




# Режим запуска определяется настройкой master
'''
Например, могут быть такие варианты:
“local” — локально на одном узле;
“local[4]” — локально с использованием четырёх узлов;
“yarn” — ресурсы управляются посредством YARN;
“spark://master:7077” — запуск в standalone mode.
'''

from pyspark.sql import SparkSession
spark = (
    SparkSession
    .builder                                    # создаём объект Spark-сессии, обращаясь к объекту builder, который создаёт сессию, учитывая параметры конфигурации
    .master("local")                            # явно указываем, что хотим запустить Spark в локальном режиме
    .appName("Python Spark SQL basic example")  # задаём название нашего Spark-приложения
    .getOrCreate()                              # функция инициализации объекта сессии
)


spark = (
    SparkSession
    .builder                                    # создаём объект Spark-сессии, обращаясь к объекту builder, который создаёт сессию, учитывая параметры конфигурации
    .config("spark.driver.memory", "10g")
    .config("spark.driver.cores", 100)
    .master("local[10]")                        # 10 nodes
    .appName("example")                         # задаём название нашего Spark-приложения
    .getOrCreate()                              # функция инициализации объекта сессии
)



# Создание датафреймов в Spark

# Способ 1. Метод createDataFrame()

import pyspark
from pyspark.sql import SparkSession

spark = (SparkSession.builder 
                    .master("local") 
                    .appName("Learning DataFrames") 
                    .getOrCreate()
)

data = [('2021-01-04', 3744, 63, 322),
        ('2021-01-04', 2434, 21, 382),
        ('2021-01-04', 2434, 32, 159),
        ('2021-01-04', 3744, 32, 159),
        ('2021-01-04', 4342, 32, 159),
        ('2021-01-04', 4342, 12, 259),
        ('2021-01-04', 5677, 12, 259),
        ('2021-01-04', 5677, 23, 499)
]

columns = ['dt', 'user_id', 'product_id', 'purchase_amount']
df = spark.createDataFrame(data=data, schema=columns)

# Проверить, верно ли Spark определил все типы данных, можно, использовав метод printSchema()

df.printSchema()
# output
'''
root
 |-- dt: string (nullable = true)
 |-- user_id: long (nullable = true)
 |-- product_id: long (nullable = true)
 |-- purchase_amount: long (nullable = true)
'''

# есть способ этим управлять, задав схему явно, то есть определить тип данных каждого столбца или атрибута.
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()

schema = StructType([
    StructField("longitude", FloatType(), nullable=True),
    StructField("latitude", FloatType(), nullable=True),
    StructField("median_age", FloatType(), nullable=True),
    StructField("total_rooms", FloatType(), nullable=True),
    StructField("total_bdrms", FloatType(), nullable=True),
    StructField("population", FloatType(), nullable=True),
    StructField("households", FloatType(), nullable=True),
    StructField("median_income", FloatType(), nullable=True),
    StructField("median_house_value", FloatType(), nullable=True)]
)

data = spark.read.csv('datasets/example.csv', schema=schema)


# Чтение 
# 1 способ
usersDF = spark.read.load(path = "examples/src/main/resources/users.parquet", format = 'parquet')

# 2 способ
df = spark.read.csv(path = "/path/file_name", sep=";", inferSchema=True, header=True)

# 3 способ
df = spark.read.parquet("people.parquet")


# Запись данных 

df.write.format(*формат*).mode(*режим записи*).save(*путь куда записывать*) 

'''
write — запись в таблицу;
format() — указание, в каком формате записать файл;
mode() — параметр режима записи. 
По умолчанию файл перезаписывается каждый раз при выполнении части кода. Или же можно указать overwrite. Чтобы дозаписать данные, указываем mode('append').
save() — путь, куда нужно записать результат.
Если подставить случайные параметры, то записать результат расчётов можно конструкцией вида:
'''
df.write.format('csv').mode('overwrite').save('dataseets/covid19_dataset/time_province_2')


'''
При большом количестве данных вы можете ускорить вычисления за счёт увеличения параллелизма. 
Для этого с помощью партиционирования можно увеличить количество Tasks. 
Tasks выполняются параллельно, поэтому чем больше партиционированы данные, тем больше данных будет обрабатываться единовременно. 
Степень параллельности однозначно определяется количеством партиций.
'''

# Для партиционирования используется функция partitionby(*колонка*)
df.write.option("header",True) \
        .partitionBy("state") \
        .mode("overwrite") \
        .parquet("/tmp/zipcodes-state")

'''
Простейшие операции с DataFrame
Каждый из перечисленных методов применяется к датафрейму в конце выражения: 
df.[набор команд (агрегация, фильтрация, соединение].[простейшие операции]
show() — используется для отображения содержимого датафрейма в формате строк и столбцов таблицы. 
По умолчанию отображается только 20 строк, а значения столбцов усекаются до 20 символов. 
Выражение будет выглядеть так: show(n = 20, truncate = True).
Следовательно, у команды две основных настройки: 
N — количество строк, truncate — параметр, который показывает, усекаются ли значения столбцов с помощью TRUE и FALSE. 
Запись show(100, False) говорит «вывести 100 строк, не усекать значения столбцов».
df.count() — выводит количество строк в датафрейме.
select(”cols”) — создаёт датафрейм из предыдущего и позволяет выбирать только определенные атрибуты — колонки. 
Пример использования: df_new = df.select(”сol_name_1”, "col_name_2").
orderBy(”сols”) — позволяет сортировать датафрейм. По умолчанию делает это по убыванию. 
Пример использования: df_new = df.select("сol_name_1","col_name_2").orderBy("col_name_2"). 
Причём передавать можно любое количество атрибутов.
distinct() — оставляет в датафрейме только уникальные строки. 
Этим же способом можно узнать уникальное содержимое одной колонки, если использовать df.distinct().show():
'''

events.select('event_type').distinct().show()
+------------+
|  event_type|
+------------+
|    reaction|
|     message|
|subscription|
+------------+

'''
collect() — возвращает все данные на драйвер в виде массива. 
Обычно это полезно после фильтра или другой операции, которая возвращает небольшое подмножество данных. 
Для RDD или датафрейма с большим количество данных это может привести к нехватке и переполнению памяти, 
поскольку collect() возвращает все данные от всех Executor драйверу.
'''


# join

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
# данные первого датафрейма 
data = [('2021-01-04', 3744, 63, 322),
        ('2021-01-04', 2434, 21, 382),
        ('2021-01-04', 2434, 32, 159),
        ('2021-01-04', 3744, 32, 159),
        ('2021-01-04', 4342, 32, 159),
        ('2021-01-04', 4342, 12, 259),
        ('2021-01-04', 5677, 12, 259),
        ('2021-01-04', 5677, 23, 499)
]
# данные второго датафрейма
data_name = [
        ( 2434, "Ваня"),
        ( 3744, "Катя" ),
        (4342, "Никита"),
        (5677, "Ваня"),
        (3879, "Юля"),
        (1398, "Ваня")+-------+----------+----------+---------------+------+
|user_id|        dt|product_id|purchase_amount|  name|
+-------+----------+----------+---------------+------+
|   2434|2021-01-04|        21|            382|  Ваня|
|   2434|2021-01-04|        32|            159|  Ваня|
|   4342|2021-01-04|        32|            159|Никита|
|   4342|2021-01-04|        12|            259|Никита|
|   5677|2021-01-04|        12|            259|  Ваня|
|   5677|2021-01-04|        23|            499|  Ваня|
|   3744|2021-01-04|        63|            322|  Катя|
|   3744|2021-01-04|        32|            159|  Катя|
+-------+----------+----------+---------------+------+
]
# названия атрибутов
columns = ['dt', 'user_id', 'product_id', 'purchase_amount']
columns_name = ['user_id', 'name']
# создаём датафреймы
df = spark.createDataFrame(data=data, schema=columns)
df_name = spark.createDataFrame(data=data_name, schema=columns_name)

# названия колонок в датафреймах одинаковы
# названия атрибутов можно передать списком
joined = df.join(df_name, ['user_id'], how='left')
joined.show()

# в случае если названия в датафреймах разные, нужно указывать атрибуты в явном виде, как
# показано ниже
joined_2 = df.join(df_name, df.user_id == df_name.user_id, how='left')
joined_2.show()

# Чтобы дублирующиеся колонки не мешались, можно удалить ненужную колонку методом drop(). 
# Важно, что надо явно указать не только название столбца, но и датафрейм, столбец которого мы хотим удалить.
joined_2 = df.join(df_name, df.user_id == df_name.user_id, how='left').drop(df.user_id)
joined_2.show()

'''
К тому же PySpark обзавёлся и новыми типами join():
anti, leftanti, left_anti — возвращают колонки только из левой таблицы со строками, которые не соединились с правой таблицей.
semi, leftsemi, left_semi — возвращают колонки и поля только из левой таблицы. В остальном работают как inner join.
'''

# union 

df.union(df_other) 

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
# данные первого датафрейма 
data = [
        ( 2434, "Bob"),
        ( 3744, "Jonh" ),
        (4342, "Bill"),
        (5677, "Jack"),
        (3879, "Ralf"),
        (1398, "Harry")
]
# данные второго датафрейма
data_name =[
        ( 4812, "Emma"),
        ( 3959, "Ava" ),
        (6859, "Charlotte"),
        (6960, "Sophia"),
        (1245, "Amelia"),
        (6960, "Isabella"),
                (1398, "Harry")
]
# названия атрибутов
columns_name = ['user_id', 'name']
# создаём датафреймы
df = spark.createDataFrame(data=data, schema=columns_name )
df_name = spark.createDataFrame(data=data_name, schema=columns_name )
# объединяем датафреймы
df_union = df.union(df_name)
df_union.show()

# Если вы хотите объединить несколько таблиц, то вызывайте union друг за другом. 
# Например, чтобы объединить три датафрейма:
df = df.union(df2).union(df3)

# Метод distinct — убрать дублирующиеся строки

df_union = df.union(df_name).distinct()
df_union.show()

# Метод unionByName — объединение с изменением порядка атрибутов

df.unionByName(df2)

# создаём датафреймы с разным порядком атрибутов
df = spark.createDataFrame(data=data, schema=['user_id', 'name'] )
df_name = spark.createDataFrame(data=data_name, schema=['name', 'user_id'])
# объединяем датафреймы 
# с неправильным порядком
df_union = df.union(df_name)
df_union.show()
# с правильным порядком
df_union = df.unionByName(df_name)
df_union.show() 


'''
Метод df.subtract(df_other) — вычитание датафреймов
В PySpark также есть команда, которая ведёт себя противоположно union, то есть вычитает из первого датафрейма второй — df.subtract(df_other).
 Остаются строки из первого датафрейма, которых нет во втором. 
Она используется нечасто, но иногда помогает сократить объём кода.
'''

# Кэширование

# В DataFrame API есть две функции, которые можно использовать для кэширования датафреймов: cache() и persist().
df_cache = df.cache()
df_persist = df.persist()

'''
cache() проводит кэширование датафрейма. 
Кэш по умолчанию сохраняется в оперативную память, пока там есть место, в противном случае — на жёсткий диск. 
В этом тоже проявляется принцип локальности данных, про который мы рассказывали во втором уроке третьей темы предыдущего спринта. 
Данные сохраняются на ту же ноду, на которой обрабатываются.

persist() тоже проводит кэширование датафрейма. 
В отличие от cache(), persist() позволяет выбрать место, куда сохранять данные. 
С помощью аргумента storageLevel можно указать, например, только жёсткий диск или только оперативную память. 
В случае с памятью место задаётся выражением df.persist(StorageLevel.MEMORY_ONLY), 
а для диска — df.persist(StorageLevel.DISK_ONLY).

Если вы видите запись df.persist(StorageLevel.MEMORY_ONLY_2), это означает репликацию данных на два узла для большей отказоустойчивости. 
Но надо следить за ресурсами. Как правило, оперативной памяти у вас и так немного.
'''

# Чтобы не занимать место в оперативной памяти и хранилище, вы можете чистить сохранённый ранее кэш с помощью функции unpersist().
dfPersist.unpersist()

# контрольная точка

'''
Контрольные точки
Контрольная точка (англ. checkpoint) — ещё один способ повышения производительности. 
Отличие от cache и persist в том, что контрольные точки не сохраняют историю создания датафрейма. 
Использование же команды мало отличается от команд кэширования.
'''

df.checkpoint()

'''
Контрольная точка копирует последний полученный датафрейм без истории создания, и дальнейшие преобразования будут идти уже на основе этого датафрейма. 
Это особенно удобно, когда с датафреймом производятся многочисленные операции. 
Следовательно, его план запроса будет большим — и грозит стать ещё больше. 
Чтобы не замедлять обработку для следующих этапов, контрольная точка будет прерывать план запроса.
Основная проблема с контрольными точками заключается в том, что Spark нужна возможность сохранять любую контрольную точку датафрейма на HDFS, 
что медленнее, чем кэширование: как мы говорили, чтение с HDFS занимает больше времени, чем из памяти. 
Поэтому нужно указывать для контрольной точки директорию, куда она будет сохраняться на HDFS. 
Директорию указывают с помощью выражения:
'''
from pyspark import SparkContext,SparkConf
sc = SparkContext.getOrCreate(SparkConf())
sc.setCheckpointDir(dirName="/content")

df_join = df_join.checkpoint()

# local checkpoint
df_join = df_join.localCheckpoint()
df_join.explain()

'''
То есть контрольная точка обладает точно таким же функционалом, что и persist(StorageLevel.DISK_ONLY).
За исключением того, что во втором случае сохраняется и сам план запроса.
'''


# Встроенные стандартные функции

import pyspark.sql.functions as F

# примеры expr выражений в PySpark

'''
https://sparkbyexamples.com/pyspark/pyspark-sql-expr-expression-function/

https://sparkbyexamples.com - сайт с примерами 
'''
df2=df.withColumn("gender", expr("CASE WHEN gender = 'M' THEN 'Male' " +
           "WHEN gender = 'F' THEN 'Female' ELSE 'unknown' END"))


'''
Тогда все встроенные стандартные функции в PySpark будут вводиться синтаксисом F.*имя функции*. 
Например, функция по определению cреднего будет записана как F.avg(), и она будет работать в рамках Spark, 
то есть будет учитывать распределённость данных и сможет работать с колонками датафрейма.
'''

'''В Spark у каждого объекта есть свой определённый тип, например, у датафрейма — pyspark.sql.dataframe. 
А все встроенные стандартные функции Spark принимают и возвращают тип pyspark.sql.Column, 
поскольку работают с колонками датафрейма.
'''

# При использовании встроенных стандартных функций всегда создаётся новый датафрейм. 
# Для его создания используется метод withColumn. Синтаксис у него простой:
df_new = df.withColumn("имя новой колонки", *вызов встроенной функции*)

 
 # работа с датами

 # current_date() — добавить поле даты
events_curr_day = events.withColumn('current_date',F.current_date())
events_curr_day.show(10, False)
# Если снова применить команду events_curr_day.show(10), то можно увидеть, что добавилось поле current_date с текущей датой.


# datediff(end, start) — показать разницу между датами
# datediff в числовом виде показывает разницу дней между конечной датой (”end”) и датой начала (”start”).

events_diff = events_curr_day.withColumn('diff', F.datediff( F.col('current_date'), F.col('date') ))


import pyspark.sql.functions as F

events = spark.read.json(path="hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events/")
events_new = (
    events
    .withColumn('hour',F.hour(F.col("event.datetime")))
    .withColumn('minute',F.minute(F.col("event.datetime")))
    .withColumn('second',F.second(F.col("event.datetime")))
    .orderBy(F.col('event.datetime').desc())
             )
events_new.show(5, False)            

'''
col('имя колонки') — очень важная функция. 
Именно с помощью неё происходит обращение к конкретной колонке существующего датафрейма. 
Если пропустить col(), скрипт выдаст ошибку, потому что Spark воспримет колонки 'current_date' и 'date' как обычные строки Python.

Можно обратиться к колонке через датафрейм и другим образом: events_curr_day.current_date. 
Эта структура тоже имеет тип pyspark.sql.Column. 
Но способ длиннее, чем F.col(), а в случае изменений поменять только название колонки внутри F.col() будет проще, чем менять ещё и название датафрейма.
В случае использования select вы можете передавать в него просто список названий, 
поэтому можно обращаться просто df.select('имя_колонки_1', 'имя колонки_2').
'''

# F.col().isNull() — проверить количество пропусков в колонке.
events.filter( F.col('event.message_from').isNull() ).count()

'''
Кстати, для поиска NaN-значений в Spark SQL вместо isNull() нужно просто использовать isNaN() — и функция сработает точно так же.
F.col().isNotNull() — проверить количество не нулевых значений в колонке
Стоит запомнить, что применить isNotNull() ко всему датафрейму нельзя, можно только к колонке.
'''



'''
Сочетание na и drop — ещё один способ удалить строки, полностью состоящие из значений NULL. 
Датафрейм обозначается так же, df, df.na — указатель на NULL-строки в датафрейме. 
drop же удаляет строки, и если не указать конкретный аргумент, то он удалит все строки со значением NULL.
'''

events.count() - events.na.drop().count()
0

# Этот же метод можно применить и к отдельной колонке. Покажем на примере event.message_from
events.count() - events.na.drop(subset='event.message_from').count()
24200856


# groupby
event_from=events.filter(F.col('event_type')=='message').groupBy(F.col('event.message_from')).count()
event_from.select(F.max('count') ).show()
# или
event_from.orderBy(F.col('count').desc()).show()

from pyspark.sql.functions import *

events.select( to_date(col('event.datetime')).alias('ts').cast("date") ).show(10,False)
data = events.withColumn("dt", to_date(col("event.datetime")))

df_dt = df_ts.withColumn('date', F.col('current_ts').cast('date'))

data.filter(F.col('event_type')=='reaction').groupBy(F.col('dt')).count().orderBy(F.col('count').desc()).show(5)

'''
Вы могли заметить, что, в случае агрегации, колонки автоматически называются max(count) или count. 
Как назвать их по-другому? Есть два варианта:
функция withColumnRenamed();
использовать withColumn при агрегации.
Например, для последнего задания код выглядел бы так:
'''

event_day_max = event_day.select(F.max('count') )
event_day_max.withColumnRenamed('max(count)', 'max_count').show()
+---------+
|max_count|
+---------+
|   310137|
+---------+



# Оконные функции в PySpark

# импортируем оконную функцию и модуль Spark Functions
from pyspark.sql.window import Window 
import pyspark.sql.functions as F

# создаём объект оконной функции
window = Window().partitionBy(*колонки партицирования*).orderBy(*колонки с возможным указанием порядка сортировки*)

# создаём колонку с применением оконной функции
df_window = df.withColumn("название новой колонки", F.*оконная функция*.over(window))

# выводим нужные колонки
df_window.select(*список нужных колонок*).show()


'''
Разберём код подробнее.
Чтобы начать работать с любой оконной функцией, нужно импортировать объект window из модуля pyspark.sql — получается pyspark.sql.window.
Через объект оконной функции window указываем поле, по которому идёт партиционирование, и наличие или отсутствие сортировки:
'''

window = Window().partitionBy(['сol']).orderBy(F.asc('col'))

'''
Задавая объект, с помощью asc указываем сортировку по возрастанию по аналогии с SQL. 
По умолчанию сортировка всегда идёт по возрастанию, поэтому на самом деле F.asc() можно не указывать, просто написав orderBy('col') или orderBy(F.col('col')). 
Противоположной функции F.asc() будет F.desc().
Если партиционировать не нужно, то можно оставить partitionBy() без атрибута или вовсе его не указывать: window = Window().orderBy(F.asc('col')).
'''

'''
Но в таком случае все данные будут обрабатываться одним узлом — это называется синглтон (англ. «singleton» — «одиночка»). 
Вот как происходит весь процесс.

Когда вы считываете датафрейм, Spark сам по базовым настройкам разбивает его на 200 партиций и, соответственно, получается 200 заданий — Tasks. 
Дальше работа идёт с 200-ми блоками. Но если применить оконную функцию, блоки объединятся по партициям и будут обрабатываться отдельно, «по окнам». 
Без партиционирования данные соединятся в одну партицию — её и будет обрабатывать один узел.
В этой ситуации есть опасность: драйвер перегружается, и, когда данных станет больше, чем позволяет хранить оперативная память, это «положит» всю систему. 
Самое время вспомнить про важный принцип, который мы уже обсуждали: степень параллельности определяется только количеством партиций в датафрейме.
Кстати, если вам не нужно партиционировать, поразмышляйте, нужно ли вообще использовать оконную функцию. 
Скорее всего, в Spark уже есть встроенная функция, которая будет работать распределённо, в отличие от непартиционированной оконной. 
Например, чтобы проставить порядковый номер каждой строке, 
можно использовать встроенную функцию monotonically_increasing_id() вместо row_number без партиционирования.
'''

# Ранжирование
'''
Для этого из модуля pyspark.sql.functions можно импортировать ряд функций, в том числе уже знакомые вам rank(), row_number(),dense_rank().
Представьте задачу ранжировать сумму покупки в течение дня. 
В итоге команда для выполнения оконной функции в PySpark будет выглядеть так:
'''
# импортируем оконную функцию и модуль Spark Functions
from pyspark.sql.window import Window 
import pyspark.sql.functions as F

# создаём объект оконной функции
window = Window().partitionBy(['dt']).orderBy(F.asc('purchase_amount'))

# создаём колонку с рассчитанной статистикой по оконной функции
df_window = df.withColumn("rank", F.rank().over(window))

# выводим нужные колонки
df_window.select('dt', 'user_id', 'rank', 'purchase_amount').show()

'''
И lag(), и lead() позволяют «сместить» строки не только на одно, но и на несколько значений назад или вперёд.
lead() в PySpark приобретает такой вид: pyspark.sql.functions.lead (col, offset=1). На входе функция принимает колонку и смещение, которое по умолчанию равно 1.
lag() в PySpark функция — pyspark.sql.functions.lag(col, offset=1). На входе принимает те же значения.
Посмотрим, как lag() применяется, на конкретном примере.
Если для нашего датафрейма с данными попытаться найти траты за предыдущую покупку.
'''

# импортируем оконную функцию и модуль Spark Functions
from pyspark.sql.window import Window 
import pyspark.sql.functions as F

# создаём объект оконной функции
window = Window().partitionBy('user_id').orderBy('dt')

# создаём колонку с рассчитанной статистикой по оконной функции
dfWithLag = df.withColumn("lag_1",F.lag("purchase_amount", 1).over(window))

# фильтруем данные
dfWithLag .select('dt','user_id', 'purchase_amount','lag_1').show()



# Агрегация
window = Window().partitionBy('user_id')
# Используя функцию avg, находим среднее значение, а sum покажет сумму всех покупок.
df_window_agg= (
    df
    .withColumn("avg",F.avg("purchase_amount").over(window))
    .withColumn("sum",F.sum("purchase_amount").over(window))
)

df_window_agg.show()


# Джоба

import sys
from datetime import date as dt
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window 
import pyspark.sql.functions as F

spark = (
        SparkSession
        .builder
        .master("local[10]")
        .config("spark.driver.memory", "3g")
        .config("spark.driver.cores", 45)
        .appName("mp_session")
        .getOrCreate()
        )

def main():

    #client_name = sys.argv[1]
    date = sys.argv[1] # за какую дату забрать
    base_input_path = sys.argv[2] # откуда забрать данные
    base_output_path = sys.argv[3] # куда положить

    # читаем данные за нужную даты и забираем откуда сказали выше
    events = spark.read.json(path=f"{base_input_path}/date={date}")

    # записываем данные
    events \
    .write \
    .partitionBy('event_type') \
    .format('parquet') \
    .save(f"{base_output_path}/date={date}")


if __name__ == "__main__":
        main()


# Как работать со spark-submit
spark-submit file.py. 'Bob' '2022-01-01' 'path/to/table/'

'''
В spark-submit можно также передать параметры, которые позволят оптимизировать запуск Spark-приложения. 
Например, определить количество ядер, объём памяти, режим запуска. 
Разберём наиболее используемые параметры для spark-submit:
Параметр files запускает несколько файлов. 
Перечисляйте запускаемые файлы после объявления этого параметра. 
Может получиться, например, такая команда:
'''
spark-submit --files file.py calculate.py metric.py 'Bob' '2022-01-01' 'path/to/table/'

'''
Параметр master определяет режим запуска Spark: какой кластер будет использоваться и как распределятся ресурсы. 
Например, могут быть такие режимы запуска:
yarn — ресурсы управляются посредством YARN;
mesos — ресурсы управляются посредством Mesos;
local — локально на одном узле;
local[4] — локально с использованием четырёх узлов.
Команда с параметром master выглядит так:
'''
spark-submit --master yarn file.py 'Bob' '2022-01-01' 'path/to/table/'

'''
Параметр deploy-mode определяет тип развёртывания приложения.
Тут всего два варианта: сlient и cluster. 
С client исполнение происходит на драйвере, с cluster — на узлах кластера.
Команда с параметром deploy-mode выглядит так:
'''
spark-submit --master yarn --deploy-mode cluster file.py 'Bob' '2022-01-01' 'path/to/table/'

'''
Если вы используете тип развёртывания client, не забудьте перед передачей скрипта проверить его и на кластере. 
В итоге скрипт будет запущен именно с типом cluster.
Параметр executor-memory определяет объём памяти процессов-исполнителей кластера.
В этом случае объём памяти Executor равен 5 Гб:
'''
spark-submit --master yarn --deploy-mode cluster --executor-memory 5g file.py 'Bob' '2022-01-01' 'path/to/table/'

'''
Параметр driver-memory задаёт объём памяти драйвера.
В этом случае объём памяти драйвера равен 1 Гб:
'''
spark-submit --driver-memory 1g file.py 'Bob' '2022-01-01' 'path/to/table/'

'''
Какие бы настройки ни были в SparkSession изначально, их можно изменить с помощью Spark Submit. 
Иерархия приоритета настроек по убыванию такая: Spark Submit — SparkContext — SparkSession.
'''

# автоматизация через AirFlow

'''
Airflow — независимая программа и не имеет никакого отношения к Hadoop. 
Чтобы она прочитала скрипт, необходимо также указать пути до папок, где лежат Spark, Java, Python и настройки Yarn, Hadoop.
В секцию с импортами надо добавить несколько строк. Это будет выглядеть так:
'''
import airflow
import os 
# импортируем модуль os, который даёт возможность работы с ОС
# указание os.environ[…] настраивает окружение

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import date, datetime

# прописываем пути
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

# задаём базовые аргументы
default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'airflow'
}

# вызываем DAG
dag = DAG("example_bash_dag",
          schedule_interval=None,
          default_args=default_args
         )

# объявляем задачу с Bash-командой, которая распечатывает дату
t1 = BashOperator(
    task_id='print_date',
    #bash_command='date',
    bash_command="spark-submit --master yarn --deploy-mode cluster /lessons/partition.py '2022-05-31' '/user/master/data/events' '/user/pavelunkno/data/events'",
    retries=3,
    dag=dag
)

t1 


'''
Оператор BashOperator у вас всегда будет под рукой. Чтобы его использовать, вам не нужна специальная инфраструктура на рабочем месте. 
Однако есть более специализированный и удобный оператор, который создан только для запуска Spark-приложений — SparkSubmitOperator.
Этот оператор используется не во всех компаниях, потому что требует подключения другого модуля — airflow.providers.apache.spark.operators.spark_submit. 
В нашем курсе этот модуль подключён, поэтому вы можете поработать со SparkSubmitOperator.
Перед написанием кода со SparkSubmitOperator подключаем Airflow к Spark и предоставим ему возможность работы в YARN. 
Для этого прописываем подключение в admin->connections в UI Airflow:
'''
'''
Тут видны параметры подключения:
ID — yarn_spark.
Тип подключения — Spark.
Хост — yarn.
'''

# Теперь можно написать код с оператором SparkSubmitOperator:
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                'owner': 'airflow',
                'start_date':datetime(2020, 1, 1),
                }

dag_spark = DAG(
                dag_id = "sparkoperator_demo",
                default_args=default_args,
                schedule_interval=None,
                )

# объявляем задачу с помощью SparkSubmitOperator
spark_submit_local = SparkSubmitOperator(
                        task_id='spark_submit_task',
                        dag=dag_spark,
                        application ='/home/user/partition_overwrite.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ["2020-05-01"]
                        conf={
                                "spark.driver.maxResultSize": "20g"
                            },
                        executor_cores = 2
                        executor_memory = '2g'
                        )

spark_submit_local

'''
Параметр	Что нужно указывать в параметре
application	- В этом параметре указывается ссылка на запускаемую джобу: application ='/home/user/partition_overwrite.py'.
conn_id	- В этом параметре определяются режимы запуска. В нашем коде указан yarn_spark, потому что мы дали такое значение ID подключения.
application_args - В этом параметре определяются аргументы, которые нужно передать вместе с запуском приложения. 
У нас это системные переменные, в которые входит дата: application_args = ["2022-05-31"]. Объём списка аргументов может быть любым.

conf - В этом параметре определяются те же настройки, что и у SparkSession или SparkContext. 
Вы можете задать количество узлов, тип сериализации и прочие настройки для Spark-приложений.

executor_cores/executor_memory - Часть настроек — количество узлов, объём памяти — вынесены прямо в параметры, 
так как они будут задаваться чаще всего. 
Но их можно задать и через conf, как мы это сделали при создании SparkContext.
'''

def input_paths(date, depth):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d') 
    return [f"/user/username/data/events/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message" for x in range(depth)]

paths = input_paths('2022-05-31', 7) # получаем пути
messages = spark.read.parquet(*paths)

all_tags = (
    messages
    .where("event.message_channel_to is not null")
    .selectExpr(["event.message_from as user", "explode(event.tags) as tag"])
    .groupBy("tag")
    .agg(F.expr("count(distinct user) as suggested_count"))
    .where("suggested_count >= 100")
)

verified_tags = spark.read.parquet("/user/master/data/snapshots/tags_verified/actual")
candidates = all_tags.join(verified_tags, "tag", "left_anti")

candidates.write.parquet('hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/pavelunkno/data/analytics/candidates_d7_pyspark')


# заливка из RAW-слоя в ODS-слой
import pyspark
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName(f"create ODS")
sc = SparkContext(conf=conf)
sql = SQLContext(sc)

events = sql.read.json("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events")
events.write \
        .partitionBy("date", "event_type") \
        .mode("overwrite") \
        .parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/*Твой логин*/data/events")




# пример DAG

import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
'owner': 'airflow',
'start_date':datetime(2020, 1, 1),
}

dag_spark = DAG(
dag_id = "datalake_etl",
default_args=default_args,
schedule_interval=None,
)

events_partitioned = SparkSubmitOperator(
task_id='events_partitioned',
dag=dag_spark,
application ='/home/pavelunkno/partition_overwrite.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "/user/master/data/events", "/user/pavelunkno/data/events"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

verified_tags_candidates_d7 = SparkSubmitOperator(
task_id='verified_tags_candidates_d7',
dag=dag_spark,
application ='/home/pavelunkno/verified_tags_candidates.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "7", "100", "/user/pavelunkno/data/events", 
"/user/master/data/snapshots/tags_verified/actual", "/user/pavelunkno/data/analytics/verified_tags_candidates_d7"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

verified_tags_candidates_d84 = SparkSubmitOperator(
task_id='verified_tags_candidates_d84',
dag=dag_spark,
application ='/home/pavelunkno/verified_tags_candidates.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "84", "1000", "/user/pavelunkno/data/events", 
"/user/master/data/snapshots/tags_verified/actual", "/user/pavelunkno/data/analytics/verified_tags_candidates_d84"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

user_interests_d7 = SparkSubmitOperator(
task_id='user_interests_d7',
dag=dag_spark,
application ='/home/pavelunkno/user_interests.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "7", "/user/pavelunkno/data/events", "/user/pavelunkno/data/analytics/user_interests_d7"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

user_interests_d28 = SparkSubmitOperator(
task_id='user_interests_d28',
dag=dag_spark,
application ='/home/pavelunkno/user_interests.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "28", "/user/pavelunkno/data/events", "/user/pavelunkno/data/analytics/user_interests_d28"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

events_partitioned >> [verified_tags_candidates_d7, verified_tags_candidates_d84, user_interests_d7, user_interests_d28]





#  broadcast hash join настройка в Jupyter

spark = SparkSession.builder \
    ...
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")\
    .config("other_param_name", "other_param_value")\
    .getOrCreate()

    '''
    Помимо spark.sql.autoBroadcastJoinThreshold, Spark позволяет указывать конкретно, что этот датасет нужно бродкастить. 
    Это можно сделать с помощью метода hint
    '''
    large_df.join(small_df.hint("broadcast"), "id", "left")



#  пример оптимизации
res = user_bd\
.select("id")\
.distinct()\
.join(events.select("event.*"), F.col("id") == F.col("message_from"), "inner")\
.groupBy("id").agg(F.count(F.when(F.expr("message like '%birthday%'"), F.lit(1))).alias("count"))\
.join(user_bd, "id", "right_outer")


# Как уменьшить влияние шаффла? как уменьшить время работы исполнения spark приложения
'''
1. Пересылать по сети меньшую часть данных к большей (broadcast hash join)

2. Уменьшить количество данных перед шаффлом (сделать distinct к примеру)

3. Изменить количество файлов, которые пишутся при шаффле
при шаффле данные распределяются по специальным партициям в соответствии с неким правилом, 
чаще всего — по хэш-функции от ключа. Но сколько будет этих партиций и, соответственно, записываемых на диск файлов? 
Это контролируется параметром spark.sql.shuffle.partitions.
Значение этого параметра по умолчанию — 200. 
Это значит, что каждый экзекьютор на Shuffle Write разобьёт прочитанную им партицию на 200 кусочков, 
а на Shuffle Read будут соединяться данные из кусочков с номером 1, с номером 2 и так далее
Чем больше spark.sql.shuffle.partitions — тем больше файлов на диске меньшего размера. 
Чем этот параметр меньше — тем меньше файлов на диске и тем они крупнее. Отсюда могут возникать проблемы:
a) Файлы шаффла слишком крупные. Они долго пишутся на диск, долго передаются по сети и долго обрабатываются после чтения.
Увидеть это можно в Spark UI на странице конкретных Stages в Event Timeline
Если Shuffle Read Time, Shuffle Write Time занимают значительное время, значит, стоит увеличить значение spark.sql.shuffle.partitions
b) Файлы шаффла слишком мелкие, и много времени уходит, чтобы писать и читать их по отдельности: 
то есть лишняя нагрузка и дополнительное время обработки одной шаффл-партиции становятся слишком заметны.
Это также можно увидеть в Event Timeline
В Spark 3.0 появилась функция автоматического определения правильного количества партиций. 
При spark.sql.adaptive.enabled=true и spark.sql.adaptive.coalescePartitions.enabled=true 
он сам определит правильное количество шаффл-партиций

4. Hа уровне данных хранить все значения для одного ключа в одной партиции
Ключевая проблема с шаффлом — нельзя заранее знать, в какой партиции какой ключ находится, 
поэтому приходится раскладывать данные «по полочкам» отдельно. 
Но что, если заранее записать их «по полочкам»? Это возможно, и такой способ называется бакетированием (от англ. bucketing — «группирование»).
Бакетирование — это, по сути, предварительный шаффл. Spark шаффлит данные один раз, при записи. 
Вместо HDFS здесь могут быть и другие файловые системы, например S3. 
Цветом обозначены не отдельные партиции, а бакеты — наборы партиций с одинаковым диапазоном выбранного ключа. 
По сути, бакеты — это материализованные шаффл-партиции. Ключ и количество бакетов фиксированы для датасета и задаются в момент записи.
Количество бакетов может не совпадать с количеством итоговых файлов: 
так как каждый экзекьютор имеет дело со своей партицией исходного датасета (до записи), 
итоговое количество файлов будет равно кол-во исходных партиций * кол-во бакетов
'''


# Параметр spark.executor.instances
'''
Этот параметр определяет количество доступных приложению экзекьюторов. 
Значение по умолчанию — 2. Пример, как его можно поставить в Jupyter
'''
spark = SparkSession.builder \
    ...
    .config("spark.executor.instances", "5")\
    ...
    .getOrCreate()


# Набор параметров spark.dynamicAllocation.
'''
В версии 3.0 введена возможность динамической аллокации без внешнего сервиса шаффла 
при заданном параметре spark.dynamicAllocation.shuffleTracking.enabled=true

Вот самые важные параметры из этого семейства:
spark.dynamicAllocation.enabled — флаг включения динамической аллокации, по умолчанию false.
spark.dynamicAllocation.minExecutors — минимальное поддерживаемое количество экзекьюторов, по умолчанию 1.
spark.dynamicAllocation.maxExecutors — максимальное поддерживаемое количество экзекьюторов, по умолчанию — бесконечность.
spark.dynamicAllocation.executorIdleTimeout — как долго должен оставаться без работы экзекьютор, чтобы быть «убитым», по умолчанию — 60 секунд.
spark.dynamicAllocation.schedulerBacklogTimeout — сколько времени должны существовать таски в ожидании, 
пока их кто-то возьмёт в работу, прежде чем запрашивать дополнительные ресурсы, по умолчанию — 1 секунда.
'''
# Вот пример, как проставить их в Jupyter:
spark = SparkSession.builder \
    ...
    .config("spark.dynamicAllocation.enabled", "true")\
    .config("spark.dynamicAllocation.minExecutors", "2")\
    .config("spark.dynamicAllocation.maxExecutors", "5")\
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s")\
    .config("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")\
    ...
    .getOrCreate()


# Параметр spark.executor.memory
'''
Этот параметр определяет, сколько памяти будет выделено на один экзекьютор, по умолчанию — 1 Гб. 
Задаётся он строкой вида 512m (512 Мб), 5g (5 Гб) и так далее. Например:
'''
spark = SparkSession.builder \
    ...
    .config("spark.executor.memory", "2866m")\
    .getOrCreate()


# Параметр spark.executor.cores
'''
Этот параметр задаёт количество ядер, выделяемых каждому экзекьютору. 
Если приложение запускается в YARN, то значение по умолчанию — 1, иначе по умолчанию берутся все доступные ядра. 
Пример, как его поставить:
'''
spark = SparkSession.builder \
    ...
    .config("spark.executor.cores", "2")\
    .getOrCreate()


'''
Container killed by YARN for exceeding memory limits.
5.5 GB of 5.5 GB physical memory used.
Consider boosting spark.yarn.executor.memoryOverhead.

«Контейнер был убит YARN, потому что превысил лимиты памяти. 5,5 Гб из 5,5 Гб физической памяти использовано. 
Попробуйте увеличить spark.yarn.executor.memoryOverhead
'''



