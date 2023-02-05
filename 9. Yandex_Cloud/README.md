# Проект 9-го спринта


**Общая схема инфраструктуры проекта DWH Cloud**

![Image alt](https://github.com/makoloff/de/blob/main/9.%20Yandex_Cloud/img/dwh_schema_cloud_infra.jpg)  


### Описание логики
**1. Слой STG**  
1.1. Читаем данные из топика Kafka  
1.2. Обогащаем прочитанные данные из Redis (наименование пользователя и ресторана)  
1.3. Загружаем данные в слой stg облачного Postgreql  
1.4. Отправляем обогащенные данные в новый топик Kafka  

**2. Слой DDS**  

Схема DWH хранилища по методологии Data Vault
![Image alt](https://github.com/makoloff/de/blob/main/9.%20Yandex_Cloud/img/dds_layer_eer_diagram.jpg)  

2.1. Читаем данные из топика из пункта 1.4.  
2.2. Загружаем данные в слой DDS по методологии Data Vault в облачный Postgresql  
2.3. Отправляем данные в новый топик  

**3. Слой CDM**  

3.1. Читаем данные из топика из пункта 2.3.  
3.2. Преобразовываем входящие данные в pandas dataframe для подсчета агрегированных значений  

форматы входящих и преобразованных датафреймов\данных следующие  
![Image alt](https://github.com/makoloff/de/blob/main/9.%20Yandex_Cloud/img/cdm_input_msg_df.jpg)  

3.3. Загружаем данные в витрины слоя CDM в облачный Postgresql  


**4. Финальный дашборд в Datalens**  
![Image alt](https://github.com/makoloff/de/blob/main/9.%20Yandex_Cloud/img/dashboard.png)






