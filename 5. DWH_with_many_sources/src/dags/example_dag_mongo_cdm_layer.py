import pandas as pd
from typing import List
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.variable import Variable
import logging
import pendulum
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
import pandas as pd
import numpy as np
from typing import List
from urllib.parse import quote_plus as quote
from pymongo.mongo_client import MongoClient
from airflow.models import Variable
import pymongo
from urllib.parse import quote_plus as quote
from pymongo.mongo_client import MongoClient
from time import localtime, strftime
import json 
import bson.objectid
import psycopg2.extras
 
 
SRC_PG_CONN_ID = 'PG_ORIGIN_BONUS_SYSTEM_CONNECTION'
TGT_PG_CONN_ID = 'PG_WAREHOUSE_CONNECTION'
 



def get_load_data_to_pg_cdm_dm_settlement_report():

    postgres_hook_trt = PostgresHook(TGT_PG_CONN_ID)
    with postgres_hook_trt.get_conn() as conn:
        conn.autocommit = False
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        #cursor.execute(last_update_ts_query)   
        #record = cursor.fetchone()
        #last_update = pd.read_sql(sql=last_update_ts_query, con=conn)
        #last_update_ts = int(record['update_ts'])
        
        cursor.execute("""
                        insert into cdm.dm_settlement_report(restaurant_id, restaurant_name, settlement_date, orders_count, 
                        orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)

                        select t.restaurant_id, 
                        t.restaurant_name,
                        t.settlement_date, 
                        t.orders_count,
                        orders_total_sum,
                        orders_bonus_payment_sum,
                        orders_bonus_granted_sum,
                        order_processing_fee,
                        restaurant_reward_sum
                        from
                        (
                            SELECT do2.restaurant_id, dr.restaurant_name, 
                            dt."date" as settlement_date
                            --extract(year from dt.date) as settlement_year, 
                            --extract(month from dt.date) as settlement_month
                            ,sum(s.count) as orders_count
                            ,sum(s.price*s.count) orders_total_sum
                            --,sum(s.total_sum) as orders_total_sum
                            ,sum(s.bonus_payment) orders_bonus_payment_sum,
                            sum(s.bonus_grant) orders_bonus_granted_sum,
                            sum(s.price*s.count)*0.25 order_processing_fee,
                            sum(s.price*s.count)*0.75 - sum(s.bonus_payment) restaurant_reward_sum
                            --,sum(s.bonus_payment) as orders_bonus_payment_sum 
                            --,sum(s.bonus_grant) as orders_bonus_granted_sum 
                            --,sum(s.total_sum * 0.25) as order_processing_fee 
                            --,sum(s.total_sum * 0.75 - bonus_payment) as restaurant_reward_sum 
                            
                            FROM dds.fct_product_sales s
                            left join dds.dm_orders do2 on do2.id = s.order_id  
                            left join dds.dm_timestamps dt on dt.id = do2.timestamp_id
                            left join dds.dm_restaurants dr on dr.id = do2.restaurant_id 
                            group by 1,2,3
                            order by 1,2,3,4,5
                        ) t
                        where t.settlement_date::Date > (select max(settlement_date)::Date FROM cdm.dm_settlement_report)
                        ;""")

        cursor.execute("""
                        update cdm.dm_settlement_report r 
                        set orders_count = t.orders_count
                        , orders_total_sum = t.orders_total_sum
                        , orders_bonus_payment_sum = t.orders_bonus_payment_sum
                        , orders_bonus_granted_sum = t.orders_bonus_granted_sum
                        , order_processing_fee = t.order_processing_fee
                        , restaurant_reward_sum = t.restaurant_reward_sum
                        from 
                            (
                                SELECT do2.restaurant_id, dr.restaurant_name, 
                                dt."date" as settlement_date
                                --extract(year from dt.date) as settlement_year, 
                                --extract(month from dt.date) as settlement_month
                                ,sum(s.count) as orders_count
                                ,sum(s.price*s.count) orders_total_sum
                                --,sum(s.total_sum) as orders_total_sum
                                ,sum(s.bonus_payment) orders_bonus_payment_sum,
                                sum(s.bonus_grant) orders_bonus_granted_sum,
                                sum(s.price*s.count)*0.25 order_processing_fee,
                                sum(s.price*s.count)*0.75 - sum(s.bonus_payment) restaurant_reward_sum
                                --,sum(s.bonus_payment) as orders_bonus_payment_sum 
                                --,sum(s.bonus_grant) as orders_bonus_granted_sum 
                                --,sum(s.total_sum * 0.25) as order_processing_fee 
                                --,sum(s.total_sum * 0.75 - bonus_payment) as restaurant_reward_sum 
                                
                                FROM dds.fct_product_sales s
                                left join dds.dm_orders do2 on do2.id = s.order_id  
                                left join dds.dm_timestamps dt on dt.id = do2.timestamp_id
                                left join dds.dm_restaurants dr on dr.id = do2.restaurant_id 
                                group by 1,2,3
                                order by 1,2,3,4,5
                            ) as t
                        where r.restaurant_id = t.restaurant_id
                        --and r.restaurant_name = t.restaurant_name
                        and r.settlement_date = t.settlement_date
            ;""")

 
        conn.commit()


def get_load_data_to_pg_cdm_dm_settlement_report_v2():

    postgres_hook_trt = PostgresHook(TGT_PG_CONN_ID)
    with postgres_hook_trt.get_conn() as conn:
        conn.autocommit = False
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        #cursor.execute(last_update_ts_query)   
        #record = cursor.fetchone()
        #last_update = pd.read_sql(sql=last_update_ts_query, con=conn)
        #last_update_ts = int(record['update_ts'])
        
        

        cursor.execute("""
            INSERT INTO cdm.dm_settlement_report (
            restaurant_id, restaurant_name, settlement_date, orders_count, 
            orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
            select
                dor.restaurant_id,
                dr.restaurant_name,
                dt.date settlement_date,
                sum(fps.count) orders_count,
                sum(fps.price*fps.count) orders_total_sum,
                sum(fps.bonus_payment) orders_bonus_payment_sum,
                sum(fps.bonus_grant) orders_bonus_granted_sum,
                sum(fps.price*fps.count)*0.25 order_processing_fee,
                sum(fps.price*fps.count)*0.75 - sum(fps.bonus_payment) restaurant_reward_sum
            from dds.fct_product_sales fps
            join dds.dm_orders dor on dor.id = fps.order_id
            join dds.dm_timestamps dt on dt.id = dor.timestamp_id
            join dds.dm_restaurants dr on dr.id = dor.restaurant_id
            group by dor.restaurant_id, dr.restaurant_name, dt.date
            ON CONFLICT (restaurant_id, settlement_date) DO UPDATE SET
                orders_count = EXCLUDED.orders_count,
                orders_total_sum = EXCLUDED.orders_total_sum,
                orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                order_processing_fee = EXCLUDED.order_processing_fee,
                restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;        
            ;""")

 
        conn.commit()


with DAG(
        'load_data_to_cdm',
        schedule_interval='0/15 * * * *',
        start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
        catchup=False,
        tags=['sprint5', 'example'],
        is_paused_upon_creation=False) as dag:

    begin = DummyOperator(task_id="begin")

 
    task_cdm_dm_settlement_report = PythonOperator(
        task_id='task_cdm_dm_settlement_report',
        python_callable=get_load_data_to_pg_cdm_dm_settlement_report_v2)


    end = DummyOperator(task_id="end")

    (begin >> task_cdm_dm_settlement_report >> end)
