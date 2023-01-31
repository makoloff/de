import datetime
from datetime import timedelta
from datetime import datetime as dtt
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'airflow',
    'start_date': dtt(2022, 10, 10),
}

current_date = dtt.today().strftime('%Y-%m-%d')


dag_spark_project_7 = DAG(
    dag_id = "datalake_etl_project_7",
    default_args=default_args,
    schedule_interval=None,
     tags=['project_7']
)


# заливка новых свежих дат на current date из raw в ods
task_1_partition = SparkSubmitOperator(
    tasK_id='task_1_partition',
    dag=dag_spark_project_7,
    application ='/home/pavelunkno/job_partition.py',
    conn_id= 'yarn_spark',
    application_args = [current_date, "/user/master/data/geo/events", "/user/pavelunkno/data/geo/events"],
    conf={
        "spark.driver.maxResultSize": "10g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)


# расчет и заливка 1й витрины в слой prod
task_2_table_1 = SparkSubmitOperator(
    task_id='task_2_table_1',
    dag=dag_spark_project_7,
    application ='/home/pavelunkno/1st_job_table.py',
    conn_id= 'yarn_spark',
    application_args = ["/user/pavelunkno/data/geo/events", "2022-05-29", "5",  "/user/prod/pavelunkno/analytics/geo/dataset_1"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 2,
    executor_memory = '2g'
)


# расчет и заливка 2й витрины в слой prod
task_3_table_2 = SparkSubmitOperator(
    task_id='task_3_table_2',
    dag=dag_spark_project_7,
    application ='/home/pavelunkno/2nd_job_table.py',
    conn_id= 'yarn_spark',
    application_args = ["/user/pavelunkno/data/geo/events", "2022-05-29", "5",  "/user/prod/pavelunkno/analytics/geo/dataset_2"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 2,
    executor_memory = '2g'
)


# расчет и заливка 3й витрины в слой prod
task_4_table_3 = SparkSubmitOperator(
    task_id='task_2_table_3',
    dag=dag_spark_project_7,
    application ='/home/pavelunkno/3rd_job_table.py',
    conn_id= 'yarn_spark',
    application_args = ["/user/pavelunkno/data/geo/events", "2022-05-29", "5",  "/user/prod/pavelunkno/analytics/geo/dataset_3"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 2,
    executor_memory = '2g'
)


task_1_partition >> [task_2_table_1 , task_3_table_2 , task_4_table_3]

