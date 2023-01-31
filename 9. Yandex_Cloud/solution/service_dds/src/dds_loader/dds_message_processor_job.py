from datetime import datetime
import time
from logging import Logger
import json
import uuid
from uuid import UUID
from time import localtime, strftime
import hashlib

from lib.pg.pg_connect import PgConnect
from lib.kafka_connect.kafka_connectors import KafkaProducer, KafkaConsumer
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                consumer: KafkaConsumer,
                producer: KafkaProducer,
                dds_repository: DdsRepository,
                logger: Logger) -> None:

        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START --------------------------------------->")

        while True:
            self._logger.info(f"{datetime.utcnow()}: Cycle Starting -------------------->")
            msg = self._consumer.consume()

            if not msg:
                self._logger.info(f"{datetime.utcnow()}: NO MESSAGE ----------------------->")
                continue
            
            if msg:
                self._logger.info(f"{datetime.utcnow()}: Message received -------------------->")
                self._logger.info(f"{datetime.utcnow()}: {msg}")


                # 1. hub restaurant
                order = msg['payload']
                restaurant_id = order['restaurant']['id']
                #restaurant_id_md5 = hashlib.md5(restaurant_id.encode('utf-8')).hexdigest()
                #rest_uuid = UUID(restaurant_id_md5)
                self._logger.info(f"{datetime.utcnow()}: creating UUID primary key for h_restaurant -------------------->")
                rest_uuid = uuid.uuid5(uuid.NAMESPACE_X500, restaurant_id)
                current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                load_src = 'stg-service-orders'
                self._logger.info(f"{datetime.utcnow()}: loading h_restaurant_insert to Postgresql -------------------->")
                self._dds_repository.h_restaurant_insert(
                    rest_uuid,
                    restaurant_id,
                    current_datetime,
                    load_src)
                self._logger.info(f"{datetime.utcnow()}: h_restaurant_insert to Postgresql sent -------------------->")

                # 2. hub user
                user_id = order['user']['id']
                self._logger.info(f"{datetime.utcnow()}: creating UUID primary key for h_user -------------------->")
                user_uuid = uuid.uuid5(uuid.NAMESPACE_X500, user_id)
                current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                self._logger.info(f"{datetime.utcnow()}: loading h_user_insert to Postgresql -------------------->")
                self._dds_repository.h_user_insert(
                    user_uuid,
                    user_id,
                    current_datetime,
                    load_src)
                self._logger.info(f"{datetime.utcnow()}: h_user_insert to Postgresql sent -------------------->")

                # 3. hub order
                order_id = order['id']
                order_dt = order['date']
                self._logger.info(f"{datetime.utcnow()}: creating UUID primary key for h_order -------------------->")
                order_uuid = uuid.uuid5(uuid.NAMESPACE_X500, str(order_id))
                current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                self._logger.info(f"{datetime.utcnow()}: loading h_order_insert to Postgresql -------------------->")
                self._dds_repository.h_order_insert(
                    order_uuid,
                    int(order_id),
                    order_dt,
                    current_datetime,
                    load_src)
                self._logger.info(f"{datetime.utcnow()}: h_order_insert to Postgresql sent -------------------->")

                # 4. hub products
                products = order['products']
                self._logger.info(f"{datetime.utcnow()}: Products Cycle starting -------------------->")
                for product in products:
                    product_id = product['id']
                    product_uuid = uuid.uuid5(uuid.NAMESPACE_X500, product_id)
                    current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                    self._logger.info(f"{datetime.utcnow()}: loading h_product_insert to Postgresql -------------------->")
                    self._dds_repository.h_product_insert(
                        product_uuid,
                        product_id,
                        current_datetime,
                        load_src)
                # 5. hub category
                    category_name = product['category']
                    category_uuid = uuid.uuid5(uuid.NAMESPACE_X500, category_name)
                    current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                    self._logger.info(f"{datetime.utcnow()}: loading h_category_insert to Postgresql -------------------->")
                    self._dds_repository.h_category_insert(
                        category_uuid,
                        category_name,
                        current_datetime,
                        load_src)
                # 6. l_order_product
                    hk_order_product_pk = uuid.uuid5(uuid.NAMESPACE_X500, str(str(order_uuid)+str(product_uuid)))
                    current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                    self._logger.info(f"{datetime.utcnow()}: loading l_order_product_insert to Postgresql -------------------->")
                    self._dds_repository.l_order_product_insert(
                        hk_order_product_pk,
                        order_uuid,
                        product_uuid,
                        current_datetime,
                        load_src)
                # 8. l_product_category
                    hk_product_category_pk = uuid.uuid5(uuid.NAMESPACE_X500, str(str(product_uuid) + str(category_uuid)))
                    current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                    self._logger.info(f"{datetime.utcnow()}: loading l_product_category_insert to Postgresql -------------------->")
                    self._dds_repository.l_product_category_insert(
                        hk_product_category_pk,
                        product_uuid,
                        category_uuid,
                        current_datetime,
                        load_src)
                # 9. l_product_restaurant    
                    hk_product_restaurant_pk = uuid.uuid5(uuid.NAMESPACE_X500, str(str(product_uuid) + str(rest_uuid)))
                    current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                    self._logger.info(f"{datetime.utcnow()}: loading l_product_restaurant_insert to Postgresql -------------------->")
                    self._dds_repository.l_product_restaurant_insert(
                        hk_product_restaurant_pk,
                        product_uuid,
                        rest_uuid,
                        current_datetime,
                        load_src)

                # 12. s_product_names
                    product_uuid_2 = uuid.uuid5(uuid.NAMESPACE_X500, str(product_uuid))
                    product_name = product['name']
                    current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                    self._logger.info(f"{datetime.utcnow()}: loading s_product_names_insert to Postgresql sent -------------------->")
                    self._dds_repository.s_product_names_insert(
                        product_uuid_2,
                        product_uuid,
                        product_name,
                        current_datetime,
                        load_src)
                    
                self._logger.info(f"{datetime.utcnow()}: ALL h_product_insert to Postgresql sent --------------------->")
                self._logger.info(f"{datetime.utcnow()}: ALL h_category_insert to Postgresql sent -------------------->")
                self._logger.info(f"{datetime.utcnow()}: ALL l_order_product_insert to Postgresql sent --------------->")
                self._logger.info(f"{datetime.utcnow()}: ALL l_product_category_insert to Postgresql sent ------------>")
                self._logger.info(f"{datetime.utcnow()}: ALL l_product_restaurant_insert to Postgresql sent ---------->")
                self._logger.info(f"{datetime.utcnow()}: ALL s_product_names_insert to Postgresql sent --------------->")


                # message loading to kafka topic dds-service-orders
                dst_msg = {
                    "id": msg["object_id"],
                    "user": str(user_uuid),
                    "products": self._format_items(msg['payload']['products'])
                    }
                
                self._producer.produce(dst_msg)
                self._logger.info(f"{datetime.utcnow()}. Message Sent to topic dds-service-orders ------------------->")
                

                # 7. l_order_user
                self._logger.info(f"{datetime.utcnow()}: l_order_user starting -------------------->")
                hk_order_user_pk = uuid.uuid5(uuid.NAMESPACE_X500, str(str(order_uuid)+str(user_uuid)))
                current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                self._logger.info(f"{datetime.utcnow()}: loading l_order_user_insert to Postgresql -------------------->")
                self._dds_repository.l_order_user_insert(
                    hk_order_user_pk,
                    order_uuid,
                    user_uuid,
                    current_datetime,
                    load_src)
                self._logger.info(f"{datetime.utcnow()}: ALL l_order_user_insert to Postgresql sent -------------------->")


                # 10. s_order_cost
                self._logger.info(f"{datetime.utcnow()}: s_order_cost starting -------------------->")
                self._logger.info(f"{datetime.utcnow()}: creating UUID primary key for s_order_cost -------------------->")
                order_uuid_2 = uuid.uuid5(uuid.NAMESPACE_X500, str(order_uuid))
                cost = order['cost']
                payment = order['payment']
                current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                self._logger.info(f"{datetime.utcnow()}: loading s_order_cost_insert to Postgresql sent -------------------->")
                self._dds_repository.s_order_cost_insert(
                    order_uuid_2,
                    order_uuid,
                    float(cost),
                    float(payment),
                    current_datetime,
                    load_src)
                self._logger.info(f"{datetime.utcnow()}: ALL s_order_cost_insert to Postgresql sent -------------------->")


                # 11. s_order_status
                self._logger.info(f"{datetime.utcnow()}: s_order_status starting -------------------->")
                self._logger.info(f"{datetime.utcnow()}: creating UUID primary key for s_order_status -------------------->")
                status = order['status']
                current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                self._logger.info(f"{datetime.utcnow()}: loading s_order_status_insert to Postgresql sent -------------------->")
                self._dds_repository.s_order_status_insert(
                    order_uuid_2,
                    order_uuid,
                    status,
                    current_datetime,
                    load_src)
                self._logger.info(f"{datetime.utcnow()}: ALL s_order_status_insert to Postgresql sent -------------------->")


                # 13. s_restaurant_names
                self._logger.info(f"{datetime.utcnow()}: s_restaurant_names starting -------------------->")
                self._logger.info(f"{datetime.utcnow()}: creating UUID primary key for s_restaurant_names -------------------->")
                rest_uuid_2 = uuid.uuid5(uuid.NAMESPACE_X500, str(rest_uuid))
                rest_name = order['restaurant']['name']
                current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                self._logger.info(f"{datetime.utcnow()}: loading s_restaurant_names_insert to Postgresql sent -------------------->")
                self._dds_repository.s_restaurant_names_insert(
                    rest_uuid_2,
                    rest_uuid,
                    rest_name,
                    current_datetime,
                    load_src)
                self._logger.info(f"{datetime.utcnow()}: ALL s_restaurant_names_insert to Postgresql sent -------------------->")


                # 14. s_user_names
                self._logger.info(f"{datetime.utcnow()}: s_user_names starting -------------------->")
                self._logger.info(f"{datetime.utcnow()}: creating UUID primary key for h_user -------------------->")
                user_uuid_2 = uuid.uuid5(uuid.NAMESPACE_X500, str(user_uuid))
                user_name = order['user']['name']
                user_login = order['user']['login']
                current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                self._logger.info(f"{datetime.utcnow()}: loading s_user_names_insert to Postgresql sent -------------------->")
                self._dds_repository.s_user_names_insert(
                    user_uuid_2,
                    user_uuid,
                    user_name,
                    user_login,
                    current_datetime,
                    load_src)
                self._logger.info(f"{datetime.utcnow()}: ALL s_user_names_insert to Postgresql sent -------------------->")


                self._logger.info(f"{datetime.utcnow()}: FINISH --------------------------------------------------->")


    def _format_items(self, products):
        items = []

        for product in products:
            product_id = product['id']
            product_uuid = uuid.uuid5(uuid.NAMESPACE_X500, product_id)
            product_name = product['name']
            category_name = product['category']
            category_uuid = uuid.uuid5(uuid.NAMESPACE_X500, category_name)
            dst_it = {
                "product_uuid": str(product_uuid),
                "product_name": product_name,
                "category_uuid": str(category_uuid),
                "category_name": category_name
            }
            items.append(dst_it)

        return items
