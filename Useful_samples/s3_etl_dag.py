from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
import requests
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator

import boto3
import requests

def get_files_from_s3(business_dt,s3_conn_name):
    # опишите тело функции
    session = boto3.session.Session()

    BUCKET_NAME = 's3-sprint3'
    # пример key = 'cohort_0/aa-nickname/TWpBeЕНуHdOUzB4TlZRd09Ub3hNem94TkFsaFlTMTBiMnh0WVdOb1pYWT0=/' - этот пример не рабочий, испоьзуйте свои значения
    
    key = 'cohort_3/nickname/TWpBeU1pMHdOaTB5TjFReE5Eb3pNVG93TndsTllXdHZiRzky/user_orders_log.csv'
    
    s3 = session.client(
        service_name=s3_conn_name,
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id = 'YCAJEWXOyY8Bmyk2eJL-hlt2K',
        aws_secret_access_key = 'YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA'
    )
    # допишите тело функции
    
    business_dt_new = business_dt.replace('-','')
    s3.download_file(
        BUCKET_NAME, 
        key, 
        f'/lessons/5. Реализация ETL в Airflow/4. Extract: как подключиться к хранилищу, чтобы получить файл/Задание 2/{business_dt_new}_user_orders_log.csv'
        )


def create_files_request(conn_name, business_dt):
    connection = BaseHook.get_connection(conn_name)
    requests.post(
        f'https://{connection.host}'
#        ,data={'business_dt': business_dt}
    )
 
 

import psycopg2

def pg_execute_query(query,conn_args):
    # дполните тело функции
    conn = psycopg2.connect(**conn_args)
    cur = conn.cursor()

    cur.execute(query, conn)
    conn.commit()
    cur.close()
    conn.close()


import pandas as pd
import psycopg2, psycopg2.extras

def load_file_to_pg(filename,pg_table,conn_args):
    ''' csv-files to pandas dataframe '''
    f = pd.read_csv(filename)
    
    ''' load data to postgres '''
    cols = ','.join(list(f.columns))
    insert_stmt = f""INSERT INTO {pg_table} ({cols}) VALUES %s""
    
    pg_conn = psycopg2.connect(**conn_args)
    cur = pg_conn.cursor()
    psycopg2.extras.execute_values(cur, insert_stmt, f.values)
    pg_conn.commit()
    cur.close()
    pg_conn.close()


default_args =  {
'depends_on_past': False,
'owner': 'airflow',
'email': ['airflow@example.com'],
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
'retry_delay': timedelta(minutes=5)
}

dag = DAG(
          'etl_update_user_data',
          default_args = default_args,
          description = 'etl_update_user_data',
          schedule_interval = '0 12 * * *',
          catchup = True,
          start_date = datetime(2022, 1, 1),
          end_date = datetime(2023, 1, 1),
          params = {'business_dt' : '{{ ds }}'}
        )

delay_task = BashOperator(
                          task_id=""delay_task"",
                          bash_command='sleep 5m',
                          dag=dag
                        )



task = PythonOperator(
    task_id='generate_report_python',
    python_callable=create_files_request,
    op_kwargs={'conn_name':'create_files_api', 'business_dt': '{{ ds }}'},
    dag=dag)

get_files_task = PythonOperator(
task_id='get_files_task', 
python_callable = get_files_from_s3,
op_kwargs={'business_dt' : '{{ ds }}', 's3_conn_name' : 's3_conn_name'},
dag=dag     
)


pg_conn = BaseHook.get_connection('pg_connection')

update_dimensions = PythonOperator(
    # дополните тело оператора
    task_id= 'update_dimensions',
    python_callable=pg_execute_query ,
    op_kwargs={'query' : 'query', 'conn_args' : pg_conn }
)

update_facts = PythonOperator(
        # дополните тело оператора
        task_id = 'update_facts',
        python_callable=pg_execute_query,
        op_kwargs = {'query' : 'query', 'conn_args' : pg_conn}
)


# замените *** на нужные значения

pg_conn = BaseHook.get_connection('pg_connection')

load_customer_research = PythonOperator(task_id='load_customer_research',
                                    python_callable=load_file_to_pg,
                                    op_kwargs={'filename': '{{ ds }}'.replace('-','') + '_customer_research.csv',
                                                'pg_table': 'staging.customer_research',
                                                'conn_args': pg_conn},
                                    dag=dag)

load_user_order_log = PythonOperator(task_id='load_user_order_log',
                                    python_callable=load_file_to_pg,
                                    op_kwargs={'filename': '{{ ds }}'.replace('-','') + '_user_orders_log.csv',
                                                'pg_table': 'staging.user_order_log',
                                                'conn_args': pg_conn},
                                    dag=dag)

load_user_activity_log = PythonOperator(task_id='load_user_activity_log',
                                    python_callable=load_file_to_pg,
                                    op_kwargs={'filename': '{{ ds }}'.replace('-','') + '_user_activity_log.csv',
                                                'pg_table': 'staging.user_activity_log',
                                                'conn_args': pg_conn},
                                    dag=dag)

load_price_log = PythonOperator(task_id='user_order_log',
                                    python_callable=load_file_to_pg,
                                    op_kwargs={'filename': '{{ ds }}'.replace('-','') + '_price_log.csv',
                                                'pg_table': 'staging.price_log',
                                                'conn_args': pg_conn},
                                    dag=dag)


delay_task >> task >> get_files_task >> [load_customer_research,
load_user_order_log,
load_user_activity_log,
load_price_log ] >> update_dimensions >> update_facts
