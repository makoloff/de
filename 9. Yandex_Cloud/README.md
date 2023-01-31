# Проект 9-го спринта

Ссылка на yandex registry ``cr.yandex/crp3hipguhp5cgbb9hm6``

**Ссылка на публичный дашборд** - ``https://datalens.yandex/e8zgevp7daho4``
(там если что периодически валится кластер PG в состояние dead)

Общая схема инфраструктуры DWH Cloud

![Image alt](https://github.com/makoloff/de/blob/main/9.%20Yandex_Cloud/img/dwh_schema_cloud_infra.jpg)


## Описание логики и вопросы
**Слой DDS**

Схема DWH по методологии Data Vault
![Image alt](https://github.com/makoloff/de/blob/main/9.%20Yandex_Cloud/img/dds_layer_eer_diagram.jpg)

5. Для случаев, когда в json-сообщении есть вложенный лист из значений я через цикл прохожу по листу в самом цикле вызываю функцию заливки в PostgreSQL, насколько это оптимально и есть ли другой способ?

**UPDATE** - а можешь поделиться куском авторского кода? там наверно наилучший вариант показан - на будущее буду знать)

**Слой CDM**

2. В топик Cdm-service-orders я отправляю только те поля, которые нужны в витрине:
``product_uuid, product_name, category_uuid, category_name, id (order_id), user (user_id)``

**UPDATE** - а тут можешь тоже показать как не через Pandas сделать? на голом питоне как ты говоришь в листы перегнать, саму конструкцию хочу глянуть)

3. Не знаю насколько этот способ корректен и оптимален. 
- Т.к. у нас нет тут Spark-a, то я решил прочитать входящие json-сообщения через Pandas pd.json_normalize
`df_nested_list = pd.json_normalize(msg, record_path = ['products'], meta = ['id', 'user'])`
- затем стандартная группировка 
`df_nested_list_product_groupby = df_nested_list.groupby(['user', 'product_uuid', 'product_name']).agg(order_cnt = ('id', 'nunique')).reset_index()`
- потом перевод значений в лист кортежей
`ins_values_product = list(map(tuple, df_nested_list_product_groupby.values))`
- и заливка этого листа через cursor.executemany в Postgresql

форматы входящих и преобразованных датафреймов\данных следующие
![Image alt](https://github.com/makoloff/de/blob/main/9.%20Yandex_Cloud/img/cdm_input_msg_df.jpg)


Финальный дашборд в Datalens
![Image alt](https://github.com/makoloff/de/blob/main/9.%20Yandex_Cloud/img/dashboard.jpg)






