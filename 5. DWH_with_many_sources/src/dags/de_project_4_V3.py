import pandas as pd
import numpy as np
import json 
import bson.objectid
import psycopg2.extras
import time
import requests
import json
import psycopg2
import logging
import pendulum
import pymongo
from typing import List
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.variable import Variable
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from urllib.parse import quote_plus as quote
from pymongo.mongo_client import MongoClient
from airflow.models import Variable
from time import localtime, strftime
from sqlalchemy import text
from airflow.hooks.base import BaseHook
from airflow.hooks.http_hook import HttpHook
import ast
from airflow.operators.sql import (
    SQLCheckOperator,
    SQLValueCheckOperator
)
#from airflow.providers.common.operators.sql import SQLColumnCheckOperator


nickname = 'MP'
cohort = '3'
api_key = '25c27781-8fde-4b30-a22e-524044a7****'
base_url = 'https://d5d04q7d963eapoe****.apigw.yandexcloud.net'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-API-KEY': api_key
}


PG_CONN_ID = 'main_pg_conn'
postgres_hook = PostgresHook(PG_CONN_ID)
example_conn =  postgres_hook.get_conn()

'''
curl --location --request GET 'https://d5d04q7d963eapoe****.apigw.yandexcloud.net/restaurants?sort_field={{ sort_field }}&sort_direction={{ sort_direction }}&limit={{ limit }}&offset={{ offset }}' --header 'X-Nickname: {{ your_nickname }}' --header 'X-Cohort: {{ your_cohort_number }}' --header 'X-API-KEY: {{ api_key }}' 
'''

def ddl_stg_restaurants():
    conn = psycopg2.connect(
                                host="localhost",
                                port=5432,
                                dbname="de",
                                user="jovyan",
                                password="jovyan"
                            )
    cur = conn.cursor()
    cur.execute(
                    """
                    CREATE TABLE if not exists stg.api_restaurants (
                        id serial NOT null, --GENERATED ALWAYS AS IDENTITY,
                        object_id varchar NOT NULL,
                        object_value text NOT NULL,
                        update_ts timestamp NOT NULL
                        ,CONSTRAINT api_restaurants_pkey PRIMARY KEY (id)
                        ,CONSTRAINT api_restaurants_object_id_uindex UNIQUE (object_id)
                    );
                    """
                )

    conn.commit()


def load_to_stg_api_restaurants():
    
    response = requests.get(f'{base_url}/restaurants', headers=headers)
    response.raise_for_status()
    #print(f"Type is {type(response.content)}")
    data = response.json()

    records = [
                {
                    "object_id" : str(d['_id']),
                    "object_value" : json.dumps(d),
                    "update_ts" : strftime("%Y-%m-%d %H:%M:%S", localtime())
                }
                for d in data
              ]

    conn = psycopg2.connect(
                                host="localhost",
                                port=5432,
                                dbname="de",
                                user="jovyan",
                                password="jovyan"
                            )

    for record in records:
        cur = conn.cursor()
        cur.execute(
                        """
                        insert into stg.api_restaurants (object_id, object_value, update_ts)
                        values (%(object_id)s, %(object_value)s, %(update_ts)s)
                        on conflict (object_id) do update set 
                        object_value = excluded.object_value
                        ,update_ts = excluded.update_ts
                        ;
                        """, record
                    )

        conn.commit()


def ddl_stg_couriers():
    conn = psycopg2.connect(
                                host="localhost",
                                port=5432,
                                dbname="de",
                                user="jovyan",
                                password="jovyan"
                            )
    cur = conn.cursor()
    cur.execute(
                    """
                    CREATE TABLE if not exists stg.api_couriers (
                    id serial NOT null, --GENERATED ALWAYS AS IDENTITY,
                    object_id varchar NOT NULL,
                    object_value text NOT NULL,
                    update_ts timestamp NOT NULL
                    ,CONSTRAINT api_couriers_pkey PRIMARY KEY (id)
                    ,CONSTRAINT api_couriers_object_id_uindex UNIQUE (object_id)
                );"""
                )

    conn.commit()

def load_to_stg_api_couriers():

    offset = 0
    limit = 50
    records = []

    while True:
    
        url = f'https://d5d04q7d963eapoe****.apigw.yandexcloud.net/couriers?offset={offset}&limit={limit}'
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        if len(data) == 0:
            break
        else:
            records.extend(data)

        offset = offset + limit

    records_loads = [
                        {
                            "object_id" : str(record['_id']),
                            "object_value" : json.dumps(record),
                            "update_ts" : strftime("%Y-%m-%d %H:%M:%S", localtime())
                        }
                        for record in records
                    ]

    conn = psycopg2.connect(
                                host="localhost",
                                port=5432,
                                dbname="de",
                                user="jovyan",
                                password="jovyan"
                            )

    for record in records_loads:
        cur = conn.cursor()
        cur.execute(
                        """
                        insert into stg.api_couriers (object_id, object_value, update_ts)
                        values (%(object_id)s, %(object_value)s, %(update_ts)s)
                        on conflict (object_id) do update set 
                        object_value = excluded.object_value,
                        update_ts = excluded.update_ts
                        ;
                        """,record
                    )

        conn.commit()


def ddl_stg_deliveries():
    conn = psycopg2.connect(
                                host="localhost",
                                port=5432,
                                dbname="de",
                                user="jovyan",
                                password="jovyan"
                            )
    cur = conn.cursor()
    cur.execute(
                    """
                    CREATE TABLE if not exists stg.api_deliveries (
                    id serial NOT null, --GENERATED ALWAYS AS IDENTITY,
                    --object_id varchar NOT NULL,
                    object_value text NOT NULL,
                    update_ts timestamp NOT NULL
                    ,CONSTRAINT api_deliveries_pkey PRIMARY KEY (id)
                    ,CONSTRAINT api_deliveries_object_value_uindex UNIQUE (object_value)
                );"""
                )

    conn.commit()


def load_to_stg_api_deliveries():
    
    offset = 0
    limit = 50


    records = []
    while True:
        
        url = f'https://d5d04q7d963eapoe****.apigw.yandexcloud.net/deliveries?offset={offset}&limit={limit}'
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        if len(data) == 0:
            break
        else:
            records.extend(data)
        offset = offset + limit

    records_loads = [
                        {
                            "object_value" : json.dumps(record),
                            "update_ts" : strftime("%Y-%m-%d %H:%M:%S", localtime())
                        }
                        for record in records
                    ]

    conn = psycopg2.connect(
                                host="localhost",
                                port=5432,
                                dbname="de",
                                user="jovyan",
                                password="jovyan"
                            )

    for record in records_loads:
        cur = conn.cursor()
        cur.execute(
                        """
                        insert into stg.api_deliveries (object_value, update_ts)
                        values (%(object_value)s, %(update_ts)s)
                        on conflict (object_value) do update set 
                        object_value = excluded.object_value,
                        update_ts = excluded.update_ts
                        ;
                        """,record
                    )

        conn.commit()


def load_to_stg_api_deliveries_incorrect_data():
    
    data1 = [{"order_id": "630a1d955690d138e853a98q", "order_ts": "2022-09-27 13:35:17.699000", "delivery_id": "uqfw028vkaezcocjxyr2qi3", "courier_id": "eux0gb8m48vrg5vjr8gn4cp", "address": "\u0423\u043b. \u041a\u0440\u0430\u0441\u043d\u0430\u044f, 7, \u043a\u0432. 391", "delivery_ts": "2022-09-27 15:17:22.654000", "rate": 1, "sum": 4093, "tip_sum": 409} ]

    records = [
                {
                  "object_value" : json.dumps(d),
                  "update_ts" : strftime("%Y-%m-%d %H:%M:%S", localtime())  
                }
                    for d in data1
              ]


    conn = psycopg2.connect(
                                host="localhost",
                                port=5432,
                                dbname="de",
                                user="jovyan",
                                password="jovyan"
                            )

    for record in records:
        cur = conn.cursor()
        cur.execute(
                        """
                        insert into stg.api_deliveries (object_value, update_ts)
                        values (%(object_value)s, %(update_ts)s)
                        on conflict (object_value) do update set 
                        object_value = excluded.object_value,
                        update_ts = excluded.update_ts
                        ;
                        """,record
                    )

        conn.commit()


def ddl_stg_deliveries_stage_2():

    postgres_hook = PostgresHook(PG_CONN_ID)
    with postgres_hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
                    """
                        CREATE TABLE if not exists stg.api_deliveries_temp (
                        --update_ts timestamp not null,
                        order_id varchar NOT NULL,
                        order_ts timestamp NOT NULL,
                        delivery_id varchar NOT NULL,
                        courier_id varchar NOT NULL
                        ,address varchar NOT NULL
                        ,delivery_ts timestamp NOT NULL
                        ,rate smallint not NUll
                        ,order_sum numeric(14, 2) not null default 0,
                        tip_sum numeric(14, 2) not null default 0
                    );
                    """
                )

        conn.commit()

def load_to_stg_api_deliveries_temp():
    
    postgres_hook = PostgresHook(PG_CONN_ID)
    with postgres_hook.get_conn() as conn:
        cur = conn.cursor()
        cur.executemany(
                    """ truncate table stg.api_deliveries_temp;
                        insert into stg.api_deliveries_temp (order_id, order_ts, delivery_id, 
                                                            courier_id, address, delivery_ts, rate, 
                                                            order_sum, tip_sum)
                        select t.*
                        from
                        (
                            select 
                            --update_ts::timestamp
                            object_value ::json->>'order_id' order_id
                            ,(object_value ::json->>'order_ts')::timestamp order_ts
                            ,object_value ::json->>'delivery_id' delivery_id
                            ,object_value ::json->>'courier_id' courier_id
                            ,object_value ::json->>'address' address
                            ,(object_value ::json->>'delivery_ts')::timestamp delivery_ts
                            ,(object_value ::json->>'rate')::smallint rate
                            ,(object_value ::json->>'sum')::numeric(14,2) order_sum
                            ,(object_value ::json->>'tip_sum')::numeric(14,2) tip_sum
                            from stg.api_deliveries
                        ) t
                        
                    ;"""
                    )
        
        conn.commit()





def ddl_dds_restaurants():

    postgres_hook = PostgresHook(PG_CONN_ID)
    with postgres_hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
                    """
                    CREATE TABLE if not exists dds.dm_api_restaurants (
                        id serial4 NOT NULL,
                        restaurant_id varchar NOT NULL,
                        restaurant_name varchar NOT NULL,
                        active_from timestamp NOT NULL,
                        active_to timestamp NOT NULL
                        ,CONSTRAINT dm_api_restaurants_pkey PRIMARY KEY (id)
                        ,CONSTRAINT dm_api_restaurants_restaurant_id_unique UNIQUE (restaurant_id)
                    );
                    """
                )

        conn.commit()



def load_to_dds_api_restaurants():
    
    postgres_hook = PostgresHook(PG_CONN_ID)
    with postgres_hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
                    """ 
                        insert into dds.dm_api_restaurants (restaurant_id, restaurant_name, active_from, active_to)
                        select 
                        object_value::json->>'_id' as restaurant_id,
                        object_value::json->>'name' as restaurant_name,
                        now() as active_from
                        ,'2099-01-01'::date as active_to
                        from stg.api_restaurants ar 
                        on conflict (restaurant_id) do update set
                        restaurant_name = excluded.restaurant_name
                        ,active_from = excluded.active_from
                        ,active_to = excluded.active_to

                    ;"""
                    )
        
        conn.commit()


def ddl_dds_couriers():

    postgres_hook = PostgresHook(PG_CONN_ID)
    with postgres_hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
                    """
                    CREATE TABLE if not exists dds.dm_api_couriers (
                        id serial4 NOT NULL,
                        courier_id varchar NOT NULL,
                        courier_name varchar NOT NULL,
                        active_from timestamp NOT NULL,
                        active_to timestamp NOT NULL
                        ,CONSTRAINT dm_api_couriers_pkey PRIMARY KEY (id)
                        ,CONSTRAINT dm_api_restaurants_courier_id_unique UNIQUE (courier_id)

                    );
                    """
                )

        conn.commit()

def load_to_dds_api_couriers():
    
    postgres_hook = PostgresHook(PG_CONN_ID)
    with postgres_hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
                    """ 
                        insert into dds.dm_api_couriers (courier_id, courier_name, active_from, active_to)
                        select 
                        object_value ::json->>'_id' as courier_id,
                        object_value ::json->>'name' as courier_name,
                        now() as active_from,
                        '2099-01-01'::date as active_to
                        from stg.api_couriers ar 
                        on conflict (courier_id) do update set
                        courier_name = excluded.courier_name
                        ,active_from = excluded.active_from
                        ,active_to = excluded.active_to
                    ;"""
                    )
        
        conn.commit()



def ddl_dds_deliveries():

    postgres_hook = PostgresHook(PG_CONN_ID)
    with postgres_hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
                    """
                        CREATE TABLE if not exists dds.dm_api_deliveries (
                        id serial4 NOT NULL,
                        update_ts timestamp not null,
                        order_id varchar NOT NULL,
                        order_ts timestamp NOT NULL,
                        delivery_id varchar NOT NULL,
                        courier_id varchar NOT NULL
                        ,address varchar NOT NULL
                        ,delivery_ts timestamp NOT NULL
                        ,rate smallint not NUll default 1
                        ,order_sum numeric(14, 2) not null default 0,
                        tip_sum numeric(14, 2) not null default 0

                        ,CONSTRAINT dm_api_deliveries_pkey PRIMARY KEY (id)
                        ,CONSTRAINT dm_api_deliveries_order_sum_check CHECK ((order_sum >= 0 ))
                        ,CONSTRAINT dm_api_deliveries_tip_sum_check CHECK ((tip_sum >= 0 ))
                        ,CONSTRAINT dm_api_deliveries_rate_check CHECK ( (rate >= 1 ) and (rate <= 5) )
                        ,CONSTRAINT dm_api_deliveries_order_id_unique UNIQUE (order_id)
                        ,CONSTRAINT dm_api_deliveries_order_ts_check check ( (order_ts::date >= '2022-01-01'::date) and (order_ts::date < '2030-01-01'::date) )
                        ,CONSTRAINT dm_api_deliveries_delivery_ts_check check ( delivery_ts::timestamp > order_ts::timestamp )
                    );
                    """
                )

        conn.commit()

def load_to_dds_api_deliveries():
    
    postgres_hook = PostgresHook(PG_CONN_ID)
    with postgres_hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
                    """ 
                        insert into dds.dm_api_deliveries (update_ts, order_id, order_ts, delivery_id, 
                                                            courier_id, address, delivery_ts, rate, 
                                                            order_sum, tip_sum)
                        select t.*
                        from
                        (
                            select 
                            update_ts::timestamp
                            ,object_value ::json->>'order_id' order_id
                            ,(object_value ::json->>'order_ts')::timestamp order_ts
                            ,object_value ::json->>'delivery_id' delivery_id
                            ,object_value ::json->>'courier_id' courier_id
                            ,object_value ::json->>'address' address
                            ,(object_value ::json->>'delivery_ts')::timestamp delivery_ts
                            ,(object_value ::json->>'rate')::smallint rate
                            ,(object_value ::json->>'sum')::numeric(14,2) order_sum
                            ,(object_value ::json->>'tip_sum')::numeric(14,2) tip_sum
                            from stg.api_deliveries
                        ) t
                        where t.order_ts::timestamp >= (select coalesce(max(order_ts), '1900-01-01'::Date)::timestamp -
                                                        - INTERVAL '7 DAYS'
                                                        from dds.dm_api_deliveries)
                        on conflict (order_id) do update set
                        update_ts = excluded.update_ts
                        ,order_ts = excluded.order_ts
                        ,delivery_id = excluded.delivery_id
                        ,courier_id = excluded.courier_id
                        ,address = excluded.address
                        ,delivery_ts = excluded.delivery_ts
                        ,rate = excluded.rate
                        ,order_sum = excluded.order_sum
                        ,tip_sum = excluded.tip_sum
                    ;"""
                    )
        
        conn.commit()


def ddl_dm_courier_ledger():

    postgres_hook = PostgresHook(PG_CONN_ID)
    with postgres_hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
                    """
                    CREATE TABLE if not exists cdm.dm_courier_ledger (
                    id serial4 NOT NULL,
                    courier_id int NOT NULL,
                    courier_name varchar NOT NULL,
                    settlement_year smallint NOT NULL,
                    settlement_month smallint NOT null,
                    orders_count int not null,
                    orders_total_sum numeric(14,2) not null default 0,
                    rate_avg float not null,
                    order_processing_fee numeric(14, 2) NOT null DEFAULT 0,
                    courier_order_sum numeric(14, 2) not null default 0,
                    courier_tips_sum numeric(14, 2) not null default 0,
                    courier_reward_sum numeric(14, 2) not null default 0,

                    CONSTRAINT dm_courier_ledger_pkey PRIMARY KEY (id),
                    CONSTRAINT dm_courier_ledger_settlement_year_check CHECK ( (settlement_year >= 2022) and (settlement_year <= 2052) ),
                    CONSTRAINT dm_courier_ledger_settlement_month_check CHECK ( ( (settlement_month >= 1) and (settlement_month <= 12) ) ),
                    CONSTRAINT dm_courier_ledger_orders_count_check CHECK ((orders_count >= 0)),
                    CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK ((orders_total_sum >= 0)),
                    CONSTRAINT dm_courier_ledger_rate_avg_check CHECK (( (rate_avg >= 1) and (rate_avg <= 5) )),
                    CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK ((order_processing_fee >= 0 )),
                    CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK ((courier_order_sum >= 0) ),
                    CONSTRAINT dm_courier_ledger_courier_tips_sum_check CHECK ((courier_tips_sum >= 0 )),
                    CONSTRAINT dm_courier_ledger_courier_reward_sum_check CHECK ((courier_reward_sum >= 0 )),
                    CONSTRAINT dm_courier_ledger_settlement_date_unique UNIQUE (courier_id, settlement_year, settlement_month)
                    );
                    """
                )

        conn.commit()


def load_to_dm_courier_ledger():
    
    postgres_hook = PostgresHook(PG_CONN_ID)
    with postgres_hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
                    """ 
                    insert into cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, 
                                                        orders_count, orders_total_sum, rate_avg, order_processing_fee,
                                                        courier_order_sum, courier_tips_sum, courier_reward_sum)
                                                        

                    select id,courier_name,settlement_year, settlement_month,
                    orders_count,orders_total_sum,rate_avg,
                    order_processing_fee
                    ,case 
                        when rate_avg < 4 then case when orders_total_sum * 0.05 < 100 then 100 else orders_total_sum * 0.05 end
                        when rate_avg >= 4 and rate_avg < 4.5 then case when orders_total_sum * 0.07 < 150 then 150 else orders_total_sum * 0.07 end
                        when rate_avg >= 4.5 and rate_avg < 4.9 then case when orders_total_sum * 0.08 < 175 then 175 else orders_total_sum * 0.08 end
                        else case when orders_total_sum * 0.1 < 200 then 200 else orders_total_sum * 0.1 end
                    end as courier_order_sum
                    ,courier_tips_sum
                    ,t.courier_tips_sum * 0.95 + 
                    case 
                        when rate_avg < 4 then case when orders_total_sum * 0.05 < 100 then 100 else orders_total_sum * 0.05 end
                        when rate_avg >= 4 and rate_avg < 4.5 then case when orders_total_sum * 0.07 < 150 then 150 else orders_total_sum * 0.07 end
                        when rate_avg >= 4.5 and rate_avg < 4.9 then case when orders_total_sum * 0.08 < 175 then 175 else orders_total_sum * 0.08 end
                        else case when orders_total_sum * 0.1 < 200 then 200 else orders_total_sum * 0.1 end
                    end
                    as courier_reward_sum
                    from
                    (
                        SELECT c.id, c.courier_name,
                        extract(year from d.order_ts) as settlement_year,
                        extract(month from d.order_ts) as settlement_month,
                        count(distinct d.order_id) as orders_count,
                        sum(d.order_sum) as orders_total_sum,
                        avg(d.rate)::numeric(14,2) as rate_avg,
                        sum(d.order_sum * 0.25) as order_processing_fee
                        --,sum() as courier_order_sum
                        ,sum(d.tip_sum) as courier_tips_sum
                        --,sum() as courier_reward_sum
                        --id, update_ts, order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, order_sum, tip_sum
                        FROM dds.dm_api_deliveries d
                        join (select * from dds.dm_api_couriers where active_to > now()) c on c.courier_id = d.courier_id
                        group by 1,2,3,4
                        order by 1,2,3,4,5,6,7
                    ) t 
                    order by 1,2,3,4,5,6,7
                    on conflict (courier_id, settlement_year, settlement_month) do update set 
                    courier_name = excluded.courier_name,
                    orders_count = excluded.orders_count,
                    orders_total_sum = excluded.orders_total_sum,
                    rate_avg = excluded.rate_avg,
                    order_processing_fee = excluded.order_processing_fee,
                    courier_order_sum = excluded.courier_order_sum,
                    courier_tips_sum = excluded.courier_tips_sum,
                    courier_reward_sum = excluded.courier_reward_sum
                    ;"""
                    )
        
        conn.commit()


with DAG(
        'de_project_4_V2',
        schedule_interval='0 0 * * *' #'0/15 * * * *',
        ,start_date =  datetime(2022, 1, 1)
        #,start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
        ,catchup=False,
        tags=['DE_project_4'],
        is_paused_upon_creation=False) as dag:

    start = DummyOperator(task_id="start")

    ddl_stg_restaurants = PythonOperator(
        task_id='ddl_stg_restaurants',
        python_callable=ddl_stg_restaurants)
    
    load_to_stg_api_restaurants = PythonOperator(
        task_id='load_to_stg_api_restaurants',
        python_callable=load_to_stg_api_restaurants)

    ddl_stg_couriers = PythonOperator(
        task_id='ddl_stg_couriers',
        python_callable=ddl_stg_couriers)

    load_to_stg_api_couriers = PythonOperator(
        task_id='load_to_stg_api_couriers',
        python_callable=load_to_stg_api_couriers)

    ddl_stg_deliveries = PythonOperator(
        task_id='ddl_stg_deliveries',
        python_callable=ddl_stg_deliveries)

    load_to_stg_api_deliveries = PythonOperator(
        task_id='load_to_stg_api_deliveries',
        python_callable=load_to_stg_api_deliveries)

    load_to_stg_api_deliveries_incorrect_data = PythonOperator(
        task_id='load_to_stg_api_deliveries_incorrect_data',
        python_callable=load_to_stg_api_deliveries_incorrect_data)

    ddl_stg_deliveries_stage_2 = PythonOperator(
        task_id='ddl_stg_deliveries_stage_2',
        python_callable=ddl_stg_deliveries_stage_2)

    load_to_stg_api_deliveries_temp = PythonOperator(
        task_id='load_to_stg_api_deliveries_temp',
        python_callable=load_to_stg_api_deliveries_temp)

    ddl_dds_restaurants = PythonOperator(
        task_id='ddl_dds_restaurants',
        python_callable=ddl_dds_restaurants)

    load_to_dds_api_restaurants = PythonOperator(
        task_id='load_to_dds_api_restaurants',
        python_callable=load_to_dds_api_restaurants)

    ddl_dds_couriers = PythonOperator(
        task_id='ddl_dds_couriers',
        python_callable=ddl_dds_couriers)

    load_to_dds_api_couriers = PythonOperator(
        task_id='load_to_dds_api_couriers',
        python_callable=load_to_dds_api_couriers)

    ddl_dds_deliveries = PythonOperator(
        task_id='ddl_dds_deliveries',
        python_callable=ddl_dds_deliveries)

    load_to_dds_api_deliveries = PythonOperator(
        task_id='load_to_dds_api_deliveries',
        python_callable=load_to_dds_api_deliveries)


    check_columns_deliveries = SQLCheckOperator(
        conn_id='main_pg_conn',
        task_id="check_columns_deliveries",
        sql="""
                select min(val) from
                (
                    select distinct 
                    least(date_check, order_sum_check, tip_sum_check, rate_check, order_ts_check) val
                    from
                    (
                        select distinct 
                        case when delivery_ts > order_ts then 1 else 0 end as date_check
                        ,case when order_sum >= 0 then 1 else 0 end as order_sum_check
                        ,case when tip_sum  >= 0 then 1 else 0 end as tip_sum_check
                        ,case when rate between 1 and 5 then 1 else 0 end as rate_check
                        ,case when order_ts between '2022-01-01'::date and '2029-12-31'::date then 1 else 0 end as order_ts_check
                        FROM stg.api_deliveries_temp
                    ) t
                ) t
        ;"""
        #,params={"pickup_datetime": "2021-01-01"},
        )


    ddl_dm_courier_ledger = PythonOperator(
        task_id='ddl_dm_courier_ledger',
        python_callable=ddl_dm_courier_ledger)

    load_to_dm_courier_ledger = PythonOperator(
        task_id='load_to_dm_courier_ledger',
        python_callable=load_to_dm_courier_ledger)



    end = DummyOperator(task_id="end")


    # (
    #     start >> 
    #     ddl_stg_restaurants >> load_to_stg_api_restaurants >> 
    #     ddl_stg_couriers >> load_to_stg_api_couriers >>
    #     ddl_stg_deliveries >> load_to_stg_api_deliveries >> load_to_stg_api_deliveries_incorrect_data >> ddl_stg_deliveries_stage_2 >> load_to_stg_api_deliveries_temp >>
    #     ddl_dds_restaurants >> load_to_dds_api_restaurants >>
    #     ddl_dds_couriers >> load_to_dds_api_couriers >>
    #     ddl_dds_deliveries >> load_to_dds_api_deliveries >> check_columns_deliveries >>
    #     ddl_dm_courier_ledger >> load_to_dm_courier_ledger >>
    #     end
    # )

  
start >> [ddl_stg_restaurants, ddl_stg_couriers, ddl_stg_deliveries]

ddl_stg_restaurants >> load_to_stg_api_restaurants
ddl_stg_couriers >> load_to_stg_api_couriers
ddl_stg_deliveries >> load_to_stg_api_deliveries >> load_to_stg_api_deliveries_incorrect_data >> ddl_stg_deliveries_stage_2 >> load_to_stg_api_deliveries_temp >> check_columns_deliveries
load_to_stg_api_deliveries_temp >> [ddl_dds_restaurants, ddl_dds_couriers, ddl_dds_deliveries]

load_to_stg_api_restaurants >> ddl_dds_restaurants >> load_to_dds_api_restaurants
load_to_stg_api_couriers >> ddl_dds_couriers >> load_to_dds_api_couriers
check_columns_deliveries >> ddl_dds_deliveries >> load_to_dds_api_deliveries 
[load_to_dds_api_restaurants, load_to_dds_api_couriers, load_to_dds_api_deliveries] >> ddl_dm_courier_ledger >> load_to_dm_courier_ledger >> end

    
