from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.decorators import dag
from airflow import DAG
from datetime import datetime, timedelta
import vertica_python
from getpass import getpass
import pandas as pd
#from airflow.operators.python_operator import PythonVirtualenvOperator
import boto3


def create_folder():
    path = os.getcwd()
    print ("The current working directory is %s" % path)
    path = '/data'
    try:
        os.mkdir(path)
    except OSError:
        print ("Creation of the directory %s failed" % path)
    else:
        print ("Successfully created the directory %s " % path)





############################### STEP 1 ############################################


# скачиваем файл из источника
def fetch_s3_file(bucket: str, key: str):

    AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
    AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"
    file_path = '/data/'+key

    print(f'file_path: {file_path}')
    print(f'file_name: {key}')

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=file_path
    )

    



############################### STEP 2 ############################################


# создаем таблицу в слое STG
def ddl_stg_group_log():
    
    conn_info = {
                    'host': '51.250.75.20', # Адрес сервера из инструкции
                    'port': 5433,
                    'user': 'UNKNOWNPAVELYANDEXRU',    # Полученный логин
                    'password': 'kWMT59eg4FpmnLV',     # пароль
                    'database': 'dwh',
                    'autocommit': True
                }

    vertica_conn = vertica_python.connect(**conn_info)
    cur = vertica_conn.cursor()
    cur.execute(
                    """
                    CREATE TABLE if not exists UNKNOWNPAVELYANDEXRU__STAGING.group_log
                    (
                        group_id int not null,
                        user_id int not null,
                        user_id_from int,
                        event varchar(10),
                        dt datetime
                        
                    )
                    order by group_id, user_id
                    PARTITION BY dt::date
                    GROUP BY calendar_hierarchy_day(dt::date, 3, 2)
                    ;
                    --ALTER TABLE UNKNOWNPAVELYANDEXRU__STAGING.group_log ADD CONSTRAINT fk_groups_admin_id 
                    --FOREIGN KEY (admin_id) REFERENCES UNKNOWNPAVELYANDEXRU__STAGING.users (id)
                    ;"""
                )
    vertica_conn.commit()
    vertica_conn.close()




############################### STEP 3 ############################################


# читаем файл, вносим изменения, фильтруем нуллы, пересохраняем и заливаем в STG
def read_load_group_log():

    conn_info = {
                    'host': '51.250.75.20', # Адрес сервера из инструкции
                    'port': 5433,
                    'user': 'UNKNOWNPAVELYANDEXRU',   # Полученный логин
                    'password': 'kWMT59eg4FpmnLV',    # пароль. 
                    'database': 'dwh',
                    'autocommit': True
                }

    vertica_conn = vertica_python.connect(**conn_info)
    cur = vertica_conn.cursor()
    
    group_log = pd.read_csv('/data/group_log.csv')
    group_log['user_id_from'] = pd.array(group_log['user_id_from'], dtype="Int64") # меняем формат, чтоб не потерять данные при заливке в вертику
    group_log = group_log.loc[ ( ~group_log['group_id'].isna() ) & ( ~group_log['user_id'].isna() ) ] # фильтруем Null
    group_log.to_csv('/data/group_log.csv', index=False, sep=',') # перезаливаем файл
    
    cur.execute(''' truncate table UNKNOWNPAVELYANDEXRU__STAGING.group_log ;''') # удаляем данные
    vertica_conn.commit()

    cur.execute(
                    """
                    COPY UNKNOWNPAVELYANDEXRU__STAGING.group_log (group_id, user_id, user_id_from, event, dt)
                    FROM LOCAL '/data/group_log.csv'
                    DELIMITER ','
                    ;"""
                )

    vertica_conn.commit()
    vertica_conn.close()




############################### STEP 4 ############################################


# создаем таблицу-линк в слое dwh
def ddl_dwh_l_user_group_activity():
    
    conn_info = {
                    'host': '51.250.75.20', # Адрес сервера из инструкции
                    'port': 5433,
                    'user': 'UNKNOWNPAVELYANDEXRU',    # Полученный логин
                    'password': 'kWMT59eg4FpmnLV',     # пароль
                    'database': 'dwh',
                    'autocommit': True
                }

    vertica_conn = vertica_python.connect(**conn_info)
    cur = vertica_conn.cursor()
    cur.execute(
                    """
                    CREATE TABLE if not exists UNKNOWNPAVELYANDEXRU__DWH.l_user_group_activity
                    (
                        hk_l_user_group_activity bigint primary key,
                        hk_user_id bigint not null CONSTRAINT fk_l_user_group_activity_user_id REFERENCES UNKNOWNPAVELYANDEXRU__DWH.h_users (hk_user_id),
                        hk_group_id bigint not null CONSTRAINT fk_l_user_group_activity_group_id REFERENCES UNKNOWNPAVELYANDEXRU__DWH.h_groups (hk_group_id),
                        load_dt datetime ,
                        load_src varchar(20)   
                    )
                    order by load_dt
                    segmented by hk_l_user_group_activity all nodes
                    PARTITION BY load_dt::date
                    GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2)
                    ;"""
                )
    vertica_conn.commit()
    vertica_conn.close()



############################### STEP 5 ############################################


# грузим в таблицу-линк данные
def load_l_user_group_activity():

    conn_info = {
                    'host': '51.250.75.20', # Адрес сервера из инструкции
                    'port': 5433,
                    'user': 'UNKNOWNPAVELYANDEXRU',   # Полученный логин
                    'password': 'kWMT59eg4FpmnLV',    # пароль. 
                    'database': 'dwh',
                    'autocommit': True
                }

    vertica_conn = vertica_python.connect(**conn_info)
    cur = vertica_conn.cursor()

    cur.execute(
                    """
                    INSERT INTO UNKNOWNPAVELYANDEXRU__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
                    select distinct
                    hash(hu.hk_user_id, hg.hk_group_id) as hk_l_user_group_activity,
                    hu.hk_user_id as hk_user_id ,
                    hg.hk_group_id as hk_group_id,
                    now() as load_dt,
                    's3' as load_src
                    from UNKNOWNPAVELYANDEXRU__STAGING.group_log as g
                    left join UNKNOWNPAVELYANDEXRU__DWH.h_users as hu on g.user_id = hu.user_id
                    left join UNKNOWNPAVELYANDEXRU__DWH.h_groups as hg on g.group_id = hg.group_id
                    where hash(hu.hk_user_id, hg.hk_group_id) not in (select hk_l_user_group_activity from UNKNOWNPAVELYANDEXRU__DWH.l_user_group_activity)
                    ;
                    """
                )

    vertica_conn.commit()
    vertica_conn.close()




############################### STEP 6 ############################################

# создаем таблицу-сателлит в слое dwh
def ddl_dwh_s_auth_history():
    
    conn_info = {
                    'host': '51.250.75.20', # Адрес сервера из инструкции
                    'port': 5433,
                    'user': 'UNKNOWNPAVELYANDEXRU',    # Полученный логин
                    'password': 'kWMT59eg4FpmnLV',     # пароль
                    'database': 'dwh',
                    'autocommit': True
                }

    vertica_conn = vertica_python.connect(**conn_info)
    cur = vertica_conn.cursor()
    cur.execute(
                    """
                    CREATE TABLE if not exists UNKNOWNPAVELYANDEXRU__DWH.s_auth_history
                    (
                        hk_l_user_group_activity bigint primary key,
                        user_id_from int ,
                        event varchar(10) not null ,
                        event_dt timestamp ,
                        load_dt datetime ,
                        load_src varchar(20)   
                    )
                    order by load_dt
                    segmented by hk_l_user_group_activity all nodes
                    PARTITION BY event_dt::date
                    GROUP BY calendar_hierarchy_day(event_dt::date, 3, 2)
                    ;"""
                )
    vertica_conn.commit()
    vertica_conn.close()


# грузим данные в таблицу-сателлит в слое dwh
def load_s_auth_history():

    conn_info = {
                    'host': '51.250.75.20', # Адрес сервера из инструкции
                    'port': 5433,
                    'user': 'UNKNOWNPAVELYANDEXRU',   # Полученный логин
                    'password': 'kWMT59eg4FpmnLV',    # пароль. 
                    'database': 'dwh',
                    'autocommit': True
                }

    vertica_conn = vertica_python.connect(**conn_info)
    cur = vertica_conn.cursor()

    cur.execute(
                    """
                    INSERT into UNKNOWNPAVELYANDEXRU__DWH.s_auth_history (hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
                    select mytemp.*
                    from
                    (
                        select
                        luga.hk_l_user_group_activity,
                        gl.user_id_from ,
                        gl.event ,
                        gl.dt as event_dt ,
                        now() as load_dt,
                        's3' as load_src
                        from UNKNOWNPAVELYANDEXRU__STAGING.group_log as gl
                        left join UNKNOWNPAVELYANDEXRU__DWH.h_groups hg on gl.group_id = hg.group_id
                        left join UNKNOWNPAVELYANDEXRU__DWH.h_users as hu on gl.user_id = hu.user_id
                        left join UNKNOWNPAVELYANDEXRU__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id
                    ) mytemp
                    WHERE NOT EXISTS (
                        SELECT 1 
                        FROM   UNKNOWNPAVELYANDEXRU__DWH.s_auth_history t
                        WHERE  t.hk_l_user_group_activity = mytemp.hk_l_user_group_activity
                    )
                    ;
                    """
                )

    vertica_conn.commit()
    vertica_conn.close()


with DAG('vertica_project_dag',schedule_interval=None, start_date=datetime(2022, 8, 30)) as dag:



    bucket_files = ['dialogs.csv' , 'users.csv' , 'groups.csv', 'group_log.csv']

    start = DummyOperator(task_id="start")

    fetch_group_log = PythonOperator(
    task_id='fetch_group_log',
    python_callable=fetch_s3_file,
    op_kwargs={'bucket' : 'sprint6', 'key': 'group_log.csv'}
    )

    ddl_stg_group_log = PythonOperator(
        task_id='ddl_stg_group_log',
        python_callable=ddl_stg_group_log)

    load_group_log = PythonOperator(
        task_id='load_group_log',
        python_callable=read_load_group_log)

    ddl_dwh_l_user_group_activity = PythonOperator(
        task_id='ddl_dwh_l_user_group_activity',
        python_callable=ddl_dwh_l_user_group_activity)

    load_l_user_group_activity = PythonOperator(
        task_id='load_l_user_group_activity',
        python_callable=load_l_user_group_activity)

    ddl_dwh_s_auth_history = PythonOperator(
        task_id='ddl_dwh_s_auth_history',
        python_callable=ddl_dwh_s_auth_history)

    load_s_auth_history = PythonOperator(
        task_id='load_s_auth_history',
        python_callable=load_s_auth_history)


    end = DummyOperator(task_id="end")


    (
        start >> 
        fetch_group_log >> ddl_stg_group_log >> load_group_log  >> 
        ddl_dwh_l_user_group_activity >> load_l_user_group_activity >> 
        ddl_dwh_s_auth_history >> load_s_auth_history >>
        end
    
    )






