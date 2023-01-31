# Проект 9-го спринта

Ссылка на yandex registry ``cr.yandex/crp3hipguhp5cgbb9hm6``

**Ссылка на публичный дашборд** - ``https://datalens.yandex/e8zgevp7daho4``
(там если что периодически валится кластер PG в состояние dead)

Общая схема инфраструктуры DWH Cloud

![Image alt](https://github.com/makoloff/de/blob/main/9.%20Yandex_Cloud/img/dwh_schema_cloud_infra.jpg)


## Описание логики и вопросы
**Слой DDS**

Схема DWH по методологии Data Vault
![Image alt](https://github.com/pamakolov/de-project-sprint-9/raw/main/jpg/dds_layer_eer_diagram.jpg)

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
![Image alt](https://github.com/pamakolov/de-project-sprint-9/raw/main/jpg/cdm_input_msg_df.jpg)















### Как работать с репозиторием
1. В вашем GitHub-аккаунте автоматически создастся репозиторий `de-project-sprint-9` после того, как вы привяжете свой GitHub-аккаунт на Платформе.
2. Скопируйте репозиторий на свой компьютер. В качестве пароля укажите ваш `Access Token`, который нужно получить на странице [Personal Access Tokens](https://github.com/settings/tokens)):
	* `git clone https://github.com/{{ username }}/de-project-sprint-9.git`
3. Перейдите в директорию с проектом: 
	* `cd de-project-sprint-9`
4. Выполните проект и сохраните получившийся код в локальном репозитории:
	* `git add .`
	* `git commit -m 'my best commit'`
5. Обновите репозиторий в вашем GitHub-аккаунте:
	* `git push origin main`
