import sys
from datetime import date as dt
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window 
import pyspark.sql.functions as F

sql = (
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

def main():
    date = sys.argv[1]
    base_input_path = sys.argv[2]
    base_output_path = sys.argv[3]

    events = sql.read.parquet(path=f"{base_input_path}/date={date}")

    events \
    .write \
    .partitionBy(['event_type','date']) \
    .mode("overwrite") \
    .format('parquet') \
    .save(f"{base_output_path}/date={date}")



if __name__ == "__main__":
        main()