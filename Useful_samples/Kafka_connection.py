import json
import time
from confluent_kafka import Consumer
from logging import Logger
import pandas as pd

def error_callback(err):
    print('Something went wrong: {}'.format(err))

def main():
# установка параметров консьюмера
    host = 'rc1a-ubhlak8r592fle1t.mdb.yandexcloud.net'
    port = '9091'
    user = 'producer_consumer'
    password = 'producer_consumer'
    cert_path = 'C:\\Users\\PaulA\\.redis\\YandexInternalRootCA.crt'
    group = 'test'

    params = {
        'bootstrap.servers': f'{host}:{port}',
        'security.protocol': 'SASL_SSL',
        'ssl.ca.location': cert_path,
        'sasl.mechanism': 'SCRAM-SHA-512',
        'sasl.username': user,
        'sasl.password': password,
        'group.id': group,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'error_cb': error_callback,
        'debug': 'all',
        'client.id': 'someclientkey'
    }

# инициализация консьюмера
    consumer = Consumer(params)

# подписка консьюмера на топик Kafka
    topic = 'transaction-service-input' #'dds-service-orders' #'stg-service-orders' #'order-service_orders'
    consumer.subscribe([topic])

# запуск бесконечного цикла
    timeout: float = 3.0
    while True:
    # получение сообщения из Kafka
        msg = consumer.poll(timeout=timeout)
        if not msg:
            print('-----------------------------EMPTY---------------------------------------------')

        # если в Kafka сообщений нет, скрипт засыпает на секунду,
        # а затем продолжает выполнение в новую итерацию

        #time.sleep(60)
        # проверка, успешно ли вычитано сообщение
            #raise Exception(msg.error())
            continue
        if msg:
            # декодирование и печать сообщения
            val = msg.value().decode()
            #order = msg['payload']
            #restaurant_id = order['restaurant']['id']
            #restaurants_name = msg['payload']['restaurant']['name']
            #print(restaurant_id)
            #print(json.loads(restaurants_name.value().decode()))
            print(json.loads(val))
            print('------------------------------------------------>')
            
            #df = pd.read_json(val)
            #data = val.value
            # data = json.loads(msg.value().decode('utf-8'))
            # df_nested_list = pd.json_normalize(data, record_path =['products'], meta=['id', 'user'])
            # df_nested_list_product_groupby = df_nested_list.groupby(['user', 'product_uuid', 'product_name']).agg(order_cnt = ('id', 'nunique')).reset_index()
            # ins_values_product = list(map(tuple, df_nested_list_product_groupby.values))
            # print('----------------------------------------------------------------------------')
            # #print(df.head())
            # print(df_nested_list.head())
            # print('----------------------------------------------------------------------------')
            # print(df_nested_list_product_groupby.head())
            # print('----------------------------------------------------------------------------')
            # print(ins_values_product)
            # print('----------------------------------------------------------------------------')

if __name__ == '__main__':
    main()