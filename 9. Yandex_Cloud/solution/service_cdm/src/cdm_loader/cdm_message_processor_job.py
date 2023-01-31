from datetime import datetime
from logging import Logger
from uuid import UUID
import pandas as pd
import json

from lib.kafka_connect import KafkaConsumer
from lib.pg.pg_connect import PgConnect
from cdm_loader.repository.cdm_repository import CdmRepository



class CdmMessageProcessor:
    def __init__(self,
                consumer: KafkaConsumer,
                cdm_repository: CdmRepository,
                logger: Logger,
                 ) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START ---------------------------------------------->")

        while True:
            self._logger.info(f"{datetime.utcnow()}: Cycle Starting -------------------->")
            msg = self._consumer.consume()

            if not msg:
                self._logger.info(f"{datetime.utcnow()}: NO MESSAGE ----------------------->")
                continue
            
            if msg:

                self._logger.info(f"{datetime.utcnow()}: Message received -------------------->")
                self._logger.info(f"{datetime.utcnow()}: getting data from json -------------------->")
                self._logger.info(f"{datetime.utcnow()}: Converting to product DF -------------------->")
                df_nested_list = pd.json_normalize(msg, record_path = ['products'], meta = ['id', 'user'])

                df_nested_list_product_groupby = df_nested_list.groupby(['user', 'product_uuid', 'product_name']).agg(order_cnt = ('id', 'nunique')).reset_index()
                ins_values_product = list(map(tuple, df_nested_list_product_groupby.values))
                self._logger.info(f"{datetime.utcnow()}: loading user_product_counters_insert to Postgresql -------------------->")
                self._cdm_repository.user_product_counters_insert(ins_values_product)
                self._logger.info(f"{datetime.utcnow()}: data user_product_counters_insert to Postgresql was sent-------------------->")

                self._logger.info(f"{datetime.utcnow()}: Converting to category DF -------------------->")
                df_nested_list_category_groupby = df_nested_list.groupby(['user', 'category_uuid', 'category_name']).agg(order_cnt = ('id', 'nunique')).reset_index()
                ins_values_category = list(map(tuple, df_nested_list_category_groupby.values))
                self._logger.info(f"{datetime.utcnow()}: loading user_category_counters_insert to Postgresql -------------------->")
                self._cdm_repository.user_category_counters_insert(ins_values_category)
                self._logger.info(f"{datetime.utcnow()}: data user_category_counters_insert to Postgresql was sent-------------------->")


            self._logger.info(f"{datetime.utcnow()}: FINISH ----------------------------------------------------->")
