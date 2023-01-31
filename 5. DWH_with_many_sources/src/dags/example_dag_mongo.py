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
 
MONGO_DB_CERTIFICATE_PATH = Variable.get("MONGO_DB_CERTIFICATE_PATH")
MONGO_DB_USER = Variable.get("MONGO_DB_USER")
MONGO_DB_PASSWORD = Variable.get("MONGO_DB_PASSWORD")
MONGO_DB_REPLICA_SET = Variable.get("MONGO_DB_REPLICA_SET")
MONGO_DB_DATABASE_NAME = Variable.get("MONGO_DB_DATABASE_NAME")
MONGO_DB_HOST = Variable.get("MONGO_DB_HOST")
 
 
class MongoConnect:
    def __init__(self,
                 cert_path: str,  # Путь до файла с сертификатом
                 user: str,  # Имя пользователя БД
                 pw: str,  # Пароль пользователя БД
                 host: List[str],  # Список хостов для подключения
                 rs: str,  # replica set.
                 auth_db: str,  # БД для аутентификации
                 main_db: str  # БД с данными
                 ) -> None:
        self.user = user
        self.pw = pw
        self.host = host
        self.replica_set = rs
        self.auth_db = auth_db
        self.main_db = main_db
        self.cert_path = cert_path
        self.client = None
        self.database = None
 
        self.set_client()
        self.set_db()
 
    def url(self) -> str:
        return 'mongodb://{user}:{pw}@{host}/?replicaSet={rs}&authSource={auth_src}'.format(
            user=quote(self.user),
            pw=quote(self.pw),
            host=','.join(self.host),
            rs=self.replica_set,
            auth_src=self.auth_db)
 
    def set_client(self):
        self.client = MongoClient(self.url(), tlsCAFile=self.cert_path)
 
    def set_db(self):
        self.database = self.client[self.main_db]
 
    def get_collection(self, collection_name: str):
        return self.database[collection_name]
 
 
def mongo_parser(x):
    if isinstance(x, datetime):
        return x.isoformat()
    elif isinstance(x, bson.objectid.ObjectId):
        return str(x)
    else:
        raise TypeError(x)


def migr_stg_ordersystem_restaurants():
    collection_name = 'restaurants'
 
    last_update_ts_query = "select coalesce(max(update_ts), '1900-01-01'::timestamp) from stg.ordersystem_restaurants"
 
    postgres_hook_trt = PostgresHook(TGT_PG_CONN_ID)
    with postgres_hook_trt.get_conn() as conn:
        cursor = conn.cursor()
        last_update_ts = pd.read_sql(sql=last_update_ts_query, con=conn)
        last_update_ts = last_update_ts.iloc[0, 0].to_pydatetime()
 
 
    mongo_connect = MongoConnect(
        cert_path=MONGO_DB_CERTIFICATE_PATH, 
        user=MONGO_DB_USER, 
        pw=MONGO_DB_PASSWORD, 
        host=[MONGO_DB_HOST], 
        rs=MONGO_DB_REPLICA_SET, 
        auth_db=MONGO_DB_DATABASE_NAME, 
        main_db=MONGO_DB_DATABASE_NAME
    )
    orders = mongo_connect.get_collection(collection_name)
 
    with postgres_hook_trt.get_conn() as conn:
        cursor = conn.cursor()
 
        for order in orders.find({"update_ts": {"$gte": last_update_ts}}).sort("update_ts"):
            columns = ['object_id', 'object_value', 'update_ts']
            values = []
 
            for column in columns:
                if column == 'object_id':
                    values.append(str(order['_id']))
                if column == 'object_value':
                    object_value = {k: v for k, v in order.items() if k not in ['_id', 'update_ts']}
                    object_value = json.dumps(object_value, default=mongo_parser)
                    values.append(object_value)
                if column == 'update_ts':
                    values.append(order[column])
 
            insert_query = """
            insert into stg.ordersystem_restaurants ({}, {}, {})
            values (%s, %s, %s)
            on conflict (object_id) do update set 
            object_value = excluded.object_value
            ,update_ts = excluded.update_ts
            """.format(*columns)
 
            cursor.execute(insert_query, values)
 
        cursor.execute("""insert into stg.srv_wf_settings (workflow_key, workflow_settings) 
            values ('migr_stg_ordersystem_restaurants', '{{ "update_ts": {} }}')""".format(values[2]))
 
        conn.commit()
 
    mongo_connect.client.close()



def migr_stg_ordersystem_orders():
    collection_name = 'orders'
 
    last_update_ts_query = "select coalesce(max(update_ts), '1900-01-01'::timestamp) from stg.ordersystem_orders"
 
    postgres_hook_trt = PostgresHook(TGT_PG_CONN_ID)
    with postgres_hook_trt.get_conn() as conn:
        cursor = conn.cursor()
        last_update_ts = pd.read_sql(sql=last_update_ts_query, con=conn)
        last_update_ts = last_update_ts.iloc[0, 0].to_pydatetime()
 
 
    mongo_connect = MongoConnect(
        cert_path=MONGO_DB_CERTIFICATE_PATH, 
        user=MONGO_DB_USER, 
        pw=MONGO_DB_PASSWORD, 
        host=[MONGO_DB_HOST], 
        rs=MONGO_DB_REPLICA_SET, 
        auth_db=MONGO_DB_DATABASE_NAME, 
        main_db=MONGO_DB_DATABASE_NAME
    )
    orders = mongo_connect.get_collection(collection_name)
 
    with postgres_hook_trt.get_conn() as conn:
        cursor = conn.cursor()
 
        for order in orders.find({"update_ts": {"$gte": last_update_ts}}).sort("update_ts"):
            columns = ['object_id', 'object_value', 'update_ts']
            values = []
 
            for column in columns:
                if column == 'object_id':
                    values.append(str(order['_id']))
                if column == 'object_value':
                    object_value = {k: v for k, v in order.items() if k not in ['_id', 'update_ts']}
                    object_value = json.dumps(object_value, default=mongo_parser)
                    values.append(object_value)
                if column == 'update_ts':
                    values.append(order[column])
 
            insert_query = """
            insert into stg.ordersystem_orders ({}, {}, {})
            values (%s, %s, %s)
            on conflict (object_id) do update set 
            object_value = excluded.object_value
            ,update_ts = excluded.update_ts
            """.format(*columns)
 
            cursor.execute(insert_query, values)
 
        cursor.execute("""insert into stg.srv_wf_settings (workflow_key, workflow_settings) 
            values ('migr_stg_ordersystem_orders', '{{ "update_ts": {} }}')""".format(values[2]))
 
        conn.commit()
 
    mongo_connect.client.close()


def migr_stg_ordersystem_users():
    collection_name = 'users'
 
    last_update_ts_query = "select coalesce(max(update_ts), '1900-01-01'::timestamp) from stg.ordersystem_users"
 
    postgres_hook_trt = PostgresHook(TGT_PG_CONN_ID)
    with postgres_hook_trt.get_conn() as conn:
        cursor = conn.cursor()
        last_update_ts = pd.read_sql(sql=last_update_ts_query, con=conn)
        last_update_ts = last_update_ts.iloc[0, 0].to_pydatetime()
 
 
    mongo_connect = MongoConnect(
        cert_path=MONGO_DB_CERTIFICATE_PATH, 
        user=MONGO_DB_USER, 
        pw=MONGO_DB_PASSWORD, 
        host=[MONGO_DB_HOST], 
        rs=MONGO_DB_REPLICA_SET, 
        auth_db=MONGO_DB_DATABASE_NAME, 
        main_db=MONGO_DB_DATABASE_NAME
    )
    orders = mongo_connect.get_collection(collection_name)
 
    with postgres_hook_trt.get_conn() as conn:
        cursor = conn.cursor()
 
        for order in orders.find({"update_ts": {"$gte": last_update_ts}}).sort("update_ts"):
            columns = ['object_id', 'object_value', 'update_ts']
            values = []
 
            for column in columns:
                if column == 'object_id':
                    values.append(str(order['_id']))
                if column == 'object_value':
                    object_value = {k: v for k, v in order.items() if k not in ['_id', 'update_ts']}
                    object_value = json.dumps(object_value, default=mongo_parser)
                    values.append(object_value)
                if column == 'update_ts':
                    values.append(order[column])
 
            insert_query = """
            insert into stg.ordersystem_users ({}, {}, {})
            values (%s, %s, %s)
            on conflict (object_id) do update set 
            object_value = excluded.object_value
            ,update_ts = excluded.update_ts
            """.format(*columns)
 
            cursor.execute(insert_query, values)
 
        cursor.execute("""insert into stg.srv_wf_settings (workflow_key, workflow_settings) 
            values ('migr_stg_ordersystem_users', '{{ "update_ts": {} }}')""".format(values[2]))
 
        conn.commit()
 
    mongo_connect.client.close()



def get_load_data_to_pg_dm_users():

    last_update_ts_query = """
    select extract(epoch from (coalesce(max(workflow_settings)::timestamp, '1900-01-01'::timestamp)))::bigint as update_ts
    from dds.srv_wf_settings 
    where workflow_key = 'dm_users' ;"""


    postgres_hook_trt = PostgresHook(TGT_PG_CONN_ID)
    with postgres_hook_trt.get_conn() as conn:
        cursor = conn.cursor()
        last_update = pd.read_sql(sql=last_update_ts_query, con=conn)
        last_update_ts = int(last_update['update_ts'])
        

        users = pd.read_sql(f"""SELECT object_id as useri_id,
                        object_value::json->>'name' as user_name,
                        object_value ::json->>'login' as user_login
                        FROM stg.ordersystem_users
                        where extract(epoch from date_trunc('seconds',update_ts::timestamp)) > {last_update_ts};""", con=conn)

        ins_values = list(map(tuple, users.values))

        cursor.executemany("""insert into dds.dm_users (user_id, user_name, user_login)
                        values(%s,%s,%s)""", ins_values)

        conn.commit()

        update_users_ts_query = """ select max(update_ts::timestamp) as update_ts from stg.ordersystem_users ;"""
        update_users = pd.read_sql(sql=update_users_ts_query, con=conn)
        update_users_ts = update_users['update_ts'].values[0]

        cursor.execute(f"""insert into dds.srv_wf_settings (workflow_key, workflow_settings) 
            values ('dm_users', '{update_users_ts}' ) ; """)
 
        conn.commit()

        

def get_load_data_to_pg_dm_restaurants():

    last_update_ts_query = """
    select extract(epoch from (coalesce(max(workflow_settings)::timestamp, '1900-01-01'::timestamp)))::bigint as update_ts
    from dds.srv_wf_settings 
    where workflow_key = 'dm_restaurants' ;"""


    postgres_hook_trt = PostgresHook(TGT_PG_CONN_ID)
    with postgres_hook_trt.get_conn() as conn:
        cursor = conn.cursor()
        last_update = pd.read_sql(sql=last_update_ts_query, con=conn)
        last_update_ts = int(last_update['update_ts'])
        

        rests = pd.read_sql(f"""
            SELECT object_id as restaurant_id, 
            object_value::json->>'name' as restaurant_name,
            date_trunc('seconds', update_ts::timestamp)  as active_from,
            '2099-12-31'::date as active_to
            FROM stg.ordersystem_restaurants
            where extract(epoch from date_trunc('seconds',update_ts::timestamp)) > {last_update_ts};""", 
            con=conn)

        ins_values = list(map(tuple, rests.values))

        cursor.executemany("""insert into dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
                        values(%s,%s,%s,%s)""", ins_values)

        conn.commit()

        update_rests_ts_query = """ select max(update_ts::timestamp) as update_ts from stg.ordersystem_restaurants ;"""
        update_rests = pd.read_sql(sql=update_rests_ts_query, con=conn)
        update_rests_ts = update_rests['update_ts'].values[0]

        cursor.execute(f"""insert into dds.srv_wf_settings (workflow_key, workflow_settings) 
            values ('dm_restaurants', '{update_rests_ts}' ) ; """)
 
        conn.commit()


def get_load_data_to_pg_dm_timestamps():

    last_update_ts_query = """
    select extract(epoch from (coalesce(max(workflow_settings)::timestamp, '1900-01-01'::timestamp)))::bigint as update_ts
    from dds.srv_wf_settings 
    where workflow_key = 'dm_timestamps' ;"""


    postgres_hook_trt = PostgresHook(TGT_PG_CONN_ID)
    with postgres_hook_trt.get_conn() as conn:
        cursor = conn.cursor()
        last_update = pd.read_sql(sql=last_update_ts_query, con=conn)
        last_update_ts = int(last_update['update_ts'])
        

        times = pd.read_sql(f"""
            select distinct
            date_trunc('seconds',(object_value::json->>'date')::timestamp) as ts,
            date_part('year', (object_value::json->>'date')::date) as year,
            date_part('month', (object_value::json->>'date')::date) as month,
            date_part('day', (object_value::json->>'date')::date) as day,
            ((object_value::json->>'date')::timestamp)::time as time,
            ((object_value::json->>'date')::timestamp)::date as date
            FROM stg.ordersystem_orders
            where extract(epoch from date_trunc('seconds',update_ts::timestamp)) > {last_update_ts};""", 
            con=conn)

        ins_values = list(map(tuple, times.values))

        cursor.executemany("""insert into dds.dm_timestamps (ts, year, month, day, time, date)
                        values(%s,%s,%s,%s,%s,%s)""", ins_values)

        conn.commit()

        update_times_ts_query = """ select max(update_ts::timestamp) as update_ts from stg.ordersystem_orders ;"""
        update_times = pd.read_sql(sql=update_times_ts_query, con=conn)
        update_times_ts = update_times['update_ts'].values[0]

        cursor.execute(f"""insert into dds.srv_wf_settings (workflow_key, workflow_settings) 
            values ('dm_timestamps', '{update_times_ts}' ) ; """)
 
        conn.commit()


def get_load_data_to_pg_dm_products():

    last_update_ts_query = """
    select extract(epoch from (coalesce(max(workflow_settings)::timestamp, '1900-01-01'::timestamp)))::bigint as update_ts
    from dds.srv_wf_settings 
    where workflow_key = 'dm_products' ;"""


    postgres_hook_trt = PostgresHook(TGT_PG_CONN_ID)
    with postgres_hook_trt.get_conn() as conn:
        cursor = conn.cursor()
        last_update = pd.read_sql(sql=last_update_ts_query, con=conn)
        last_update_ts = int(last_update['update_ts'])
        

        prods = pd.read_sql(f"""
            SELECT distinct dr.id as restaurant_id
            ,json_array_elements((object_value::json->'menu'))::json->>'_id' as product_id
            ,json_array_elements((object_value::json->'menu'))::json->>'name' as product_name
            ,json_array_elements((object_value::json->'menu'))::json->>'price' as product_price
            ,date_trunc('seconds',update_ts::timestamp) as active_from
            ,'2099-12-31'::date as active_to
            FROM stg.ordersystem_restaurants r
            left join dds.dm_restaurants dr on dr.restaurant_id =r.object_id
            where extract(epoch from date_trunc('seconds',update_ts::timestamp)) > {last_update_ts};""", 
            con=conn)

        ins_values = list(map(tuple, prods.values))

        cursor.executemany("""insert into dds.dm_products (restaurant_id, product_id, product_name, 
                                                     product_price, active_from, active_to)
                        values(%s,%s,%s,%s,%s,%s)""", ins_values)

        conn.commit()

        update_prods_ts_query = """ select max(update_ts::timestamp) as update_ts from stg.ordersystem_restaurants ;"""
        update_prods = pd.read_sql(sql=update_prods_ts_query, con=conn)
        update_prods_ts = update_prods['update_ts'].values[0]

        cursor.execute(f"""insert into dds.srv_wf_settings (workflow_key, workflow_settings) 
            values ('dm_products', '{update_prods_ts}' ) ; """)
 
        conn.commit()


def get_load_data_to_pg_dm_orders():

    last_update_ts_query = """
    select extract(epoch from (coalesce(max(workflow_settings)::timestamp, '1900-01-01'::timestamp)))::bigint as update_ts
    from dds.srv_wf_settings 
    where workflow_key = 'dm_orders' ;"""


    postgres_hook_trt = PostgresHook(TGT_PG_CONN_ID)
    with postgres_hook_trt.get_conn() as conn:
        cursor = conn.cursor()
        last_update = pd.read_sql(sql=last_update_ts_query, con=conn)
        last_update_ts = int(last_update['update_ts'])
        

        orders = pd.read_sql(f"""
            select
            du.id as user_id,
            dr.id as restaurant_id, 
            dt.id as timestamp_id,
            t.order_key, 
            t.order_status
            from(
                SELECT 
                (object_value::json->'user')::json->>'id' as user_id
                ,(object_value::json->'restaurant')::json->>'id' as restaurant_id
                ,date_trunc('seconds', update_ts) as update_ts
                ,date_trunc('second',(object_value::json->>'date')::timestamp) as order_date
                ,date_trunc('second',(object_value::json->>'date')::timestamp)::time as time_o
                ,date_part('year',(object_value::json->>'date')::timestamp) as year
                ,date_part('month',(object_value::json->>'date')::timestamp) as month
                ,date_part('day',(object_value::json->>'date')::timestamp) as day
                ,date_part('hour',(object_value::json->>'date')::timestamp) as hour
                ,date_part('minute',(object_value::json->>'date')::timestamp) as mins
                ,date_part('seconds',(object_value::json->>'date')::timestamp)::int as secs
                ,object_value::json->>'final_status' as order_status
                ,object_id  as order_key
                FROM stg.ordersystem_orders o
                where extract(epoch from date_trunc('seconds',update_ts::timestamp)) > {last_update_ts}
            ) t
            left join dds.dm_restaurants dr on dr.restaurant_id = t.restaurant_id
            left join (select *, date_part('hour',time) as hour, date_part('minute',time) as mins,
                        date_part('seconds',time)::int as secs
                        from dds.dm_timestamps dt
                      ) dt on dt.year = t.year and dt.month=t.month and dt.day=t.day and dt.hour=t.hour and dt.mins=t.mins
                      and dt.secs=t.secs
            left join dds.dm_users du on du.user_id = t.user_id
            ;""", 
            con=conn)

        ins_values = list(map(tuple, orders.values))

        cursor.executemany("""insert into dds.dm_orders (user_id, restaurant_id, timestamp_id, order_key, order_status)
                        values(%s,%s,%s,%s,%s)""", ins_values)

        conn.commit()

        update_orders_ts_query = """ select max(update_ts::timestamp) as update_ts from stg.ordersystem_orders ;"""
        update_orders = pd.read_sql(sql=update_orders_ts_query, con=conn)
        update_orders_ts = update_orders['update_ts'].values[0]

        cursor.execute(f"""insert into dds.srv_wf_settings (workflow_key, workflow_settings) 
            values ('dm_orders', '{update_orders_ts}' ) ; """)
 
        conn.commit()



def get_load_data_to_pg_fct_product_sales():

    last_update_ts_query = """
    select extract(epoch from (coalesce(max(workflow_settings)::timestamp, '1900-01-01'::timestamp)))::bigint as update_ts
    from dds.srv_wf_settings 
    where workflow_key = 'fct_product_sales' ;"""


    postgres_hook_trt = PostgresHook(TGT_PG_CONN_ID)
    with postgres_hook_trt.get_conn() as conn:
        cursor = conn.cursor()
        last_update = pd.read_sql(sql=last_update_ts_query, con=conn)
        last_update_ts = int(last_update['update_ts'])
        

        data = pd.read_sql(f"""
            select p.id as product_id, do2.id as order_id, t.count, t.price, t.total_sum, t.bonus_payment, t.bonus_grant
            --t.*, do2.id , p.id
            --count(*) --t.*, do2.id 
            from 
            (
                SELECT --id, 
                event_ts --event_type, event_value
                ,event_value ::json->>'order_id' as order_id
                ,json_array_elements(event_value ::json->'product_payments')::json->>'product_id' as product_id
                ,json_array_elements(event_value ::json->'product_payments')::json->>'price' as price
                ,json_array_elements(event_value ::json->'product_payments')::json->>'quantity' as count
                ,json_array_elements(event_value ::json->'product_payments')::json->>'product_cost' as total_sum
                ,json_array_elements(event_value ::json->'product_payments')::json->>'bonus_payment' as bonus_payment
                ,json_array_elements(event_value ::json->'product_payments')::json->>'bonus_grant' as bonus_grant
                FROM stg.bonussystem_events
                where event_type ='bonus_transaction' and
                extract(epoch from date_trunc('seconds',event_ts::timestamp)) > {last_update_ts}
            ) t
            join dds.dm_orders do2 on do2.order_key =t.order_id
            join dds.dm_products p on p.product_id =t.product_id
            ;""", 
            con=conn)

        ins_values = list(map(tuple, data.values))

        cursor.executemany("""insert into dds.fct_product_sales (product_id, order_id, count, price, 
                                                                total_sum, bonus_payment, bonus_grant)
                        values(%s,%s,%s,%s,%s,%s,%s)""", ins_values)

        conn.commit()

        update_data_ts_query = """ select max(event_ts::timestamp) as update_ts from stg.bonussystem_events ;"""
        update_data = pd.read_sql(sql=update_data_ts_query, con=conn)
        update_data_ts = update_data['update_ts'].values[0]

        cursor.execute(f"""insert into dds.srv_wf_settings (workflow_key, workflow_settings) 
            values ('fct_product_sales', '{update_data_ts}' ) ; """)
 
        conn.commit()


def get_load_data_to_pg_fct_product_sales_v2():

    last_update_ts_query = """
    select extract(epoch from (coalesce(max(workflow_settings)::timestamp, '1900-01-01'::timestamp)))::bigint as update_ts
    from dds.srv_wf_settings 
    where workflow_key = 'fct_product_sales' ;"""


    postgres_hook_trt = PostgresHook(TGT_PG_CONN_ID)
    with postgres_hook_trt.get_conn() as conn:
        conn.autocommit = False
        #cursor = conn.cursor()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(last_update_ts_query)   
        record = cursor.fetchone()
        #last_update = pd.read_sql(sql=last_update_ts_query, con=conn)
        last_update_ts = int(record['update_ts'])
        

        cursor.execute(f"""
            insert into dds.fct_product_sales (product_id, order_id, count, price, 
                                                                total_sum, bonus_payment, bonus_grant)

            select p.id as product_id, do2.id as order_id, t.count::int, t.price::numeric(14, 2), 
            t.total_sum::numeric(14, 2), t.bonus_payment::numeric(14, 2), t.bonus_grant::numeric(14, 2)
            --t.*, do2.id , p.id
            --count(*) --t.*, do2.id 
            from 
            (
                SELECT --id, 
                event_ts --event_type, event_value
                ,event_value ::json->>'order_id' as order_id
                ,json_array_elements(event_value ::json->'product_payments')::json->>'product_id' as product_id
                ,json_array_elements(event_value ::json->'product_payments')::json->>'price' as price
                ,json_array_elements(event_value ::json->'product_payments')::json->>'quantity' as count
                ,json_array_elements(event_value ::json->'product_payments')::json->>'product_cost' as total_sum
                ,json_array_elements(event_value ::json->'product_payments')::json->>'bonus_payment' as bonus_payment
                ,json_array_elements(event_value ::json->'product_payments')::json->>'bonus_grant' as bonus_grant
                FROM stg.bonussystem_events
                where event_type ='bonus_transaction' and
                extract(epoch from date_trunc('seconds',event_ts::timestamp)) > {last_update_ts}
            ) t
            join dds.dm_orders do2 on do2.order_key =t.order_id
            join dds.dm_products p on p.product_id =t.product_id
            ;""")

        
        cursor.execute("""
            insert into dds.srv_wf_settings (workflow_key, workflow_settings) 
            values ('fct_product_sales', (select max(event_ts::timestamp) as update_ts from stg.bonussystem_events) ) 
            ;""")

 
        conn.commit()


with DAG(
        'load_data_from_mongo',
        schedule_interval='0/15 * * * *',
        start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
        catchup=False,
        tags=['sprint5', 'example'],
        is_paused_upon_creation=False) as dag:

    begin = DummyOperator(task_id="begin")

 
    task_restaurants = PythonOperator(
        task_id='task_restaurants',
        python_callable=migr_stg_ordersystem_restaurants)

    task_orders = PythonOperator(
        task_id='task_orders',
        python_callable=migr_stg_ordersystem_orders)

    task_users = PythonOperator(
        task_id='task_users',
        python_callable=migr_stg_ordersystem_users)

    task_dm_users = PythonOperator(
        task_id='task_dm_users',
        python_callable=get_load_data_to_pg_dm_users)

    task_dm_restaurants = PythonOperator(
        task_id='task_dm_restaurants',
        python_callable=get_load_data_to_pg_dm_restaurants)

    task_dm_timestamps = PythonOperator(
        task_id='task_dm_timestamps',
        python_callable=get_load_data_to_pg_dm_timestamps)

    task_dm_products = PythonOperator(
        task_id='task_dm_products',
        python_callable=get_load_data_to_pg_dm_products)

    task_dm_orders = PythonOperator(
        task_id='task_dm_orders',
        python_callable=get_load_data_to_pg_dm_orders)

    task_fct_product_sales = PythonOperator(
        task_id='task_fct_product_sales',
        python_callable=get_load_data_to_pg_fct_product_sales_v2)


    end = DummyOperator(task_id="end")

    (begin >>  [task_restaurants, task_orders, task_users] >> 
        task_dm_users >> task_dm_restaurants >> task_dm_timestamps >>
        task_dm_products >> task_dm_orders >> task_fct_product_sales >>
    end)
