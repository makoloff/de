import uuid
from datetime import datetime
from typing import Any, Dict, List
from uuid import UUID

from lib.pg import PgConnect
from pydantic import BaseModel


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def user_category_counters_insert(self,
                            ins_values
                            ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    insert into cdm.user_category_counters (user_id, category_id, category_name, order_cnt)
                    values(%s,%s,%s,%s)
                    on conflict (user_id, category_id) do update set 
                    category_name = excluded.category_name,
                    order_cnt = excluded.order_cnt
                    """, ins_values)

    
    def user_product_counters_insert(self,
                            ins_values
                            ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    insert into cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
                    values(%s,%s,%s,%s)
                    on conflict (user_id, product_id) do update set 
                    product_name = excluded.product_name,
                    order_cnt = excluded.order_cnt
                    """, ins_values)
