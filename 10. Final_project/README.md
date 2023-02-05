# Итоговый проект

### Варианты исполнения  
![Image alt](https://github.com/makoloff/de/blob/main/10.%20Final_project/img/infra.png)  

## Описание логики через Spark Streming  
**DAG №1.**  
1.1. Создание таблиц в слое STG Postgresql для заливки сырых данных as is  
1.2. Чтение сообщений из топика Kafka посредством Spark Streaming  
1.3. Десериализация сообщений и загрузка каждого df в соответствующую таблицу в слой STG Postgresql  
![Image alt](https://github.com/makoloff/de/blob/main/10.%20Final_project/img/1st_dag.jpg)  

**DAG № 2.**  
2.1. Запуск DAG-а через сенсор 1-го DAG-а  
2.2. Создание таблиц в слое STG в Vertica  
2.3. Заливка данных через поток битовых данных pandas df в таблицы _raw и затем в таргетную таблицу на Vertica (для дедупликации)  
2.4. Загрузка расчетов свежего дня в таргетную витрину в слое CDM  
![Image alt](https://github.com/makoloff/de/blob/main/10.%20Final_project/img/2nd_dag.jpg)  

**Часть 3. Дашборд в Metabase.**    
![Image alt](https://github.com/makoloff/de/blob/main/10.%20Final_project/img/dashboard.png)  




### Описание задачи  
Команда аналитиков попросила вас собрать данные по транзакционной активности пользователей и настроить обновление таблицы с курсом валют.   
Цель — понять, как выглядит динамика оборота всей компании и что приводит к его изменениям.  
Данные подготовлены в различных вариантах, так как в компании использовались разные протоколы передачи информации. Поэтому вы можете самостоятельно выбрать источник и решить, каким образом реализовать поставку данных в хранилище.  
Варианты размещения данных:  
- в S3;
- в PostgreSQL;
- через Spark Streaming в Kafka.

**Установка инфраструктуры**  
Инструменты для проекта вы можете развернуть двумя способами:  
1. Локальная инфраструктура: **Docker**  
Выполнив следующую команду, вы скопируете и запустите актуальный Docker-образ:  
`docker run -d -p 8998:8998 -p 8280:8280 -p 15432:5432 --name=de-final-prj-local sindb/de-final-prj:latest`  

Информация по размещению инструментов есть внутри образа, в частности:  
- Адрес Metabase — 8998. Когда перейдёте по адресу `http://localhost:8998/`, вы попадёте на страницу регистрации. Нужно будет указать имя пользователя и пароль.  
- Адрес Airflow — 8280. Когда перейдёте по адресу `http://localhost:8280/airflow/`, вы попадёте в меню авторизации: пользователь — `AirflowAdmin`; пароль — `airflow_pass`.  
- Данные по подключению к PostgreSQL: пользователь — `jovyan`; пароль — `jovyan`  

Вы можете запустить поставку данных в режиме реального времени в два топика Kafka. Инициируйте запуск топиков через curl-вызов:  
`curl -X POST https://order-gen-service.sprint9.tgcloudenv.ru/project/register_kafka \
-H 'Content-Type: application/json; charset=utf-8' \
--data-binary @- << EOF
{
    "student": "ваш_логин",
    "kafka_connect":{
        "host": "ваш_хост_kafka",
        "port": 9091,
        "topic": "transaction-service-input",
        "producer_name": "producer_consumer",
        "producer_password": "пароль_от_продюсера_к_вашей_kafka"
    }
}
EOF`  
К вам будут поступать сообщения двух типов:  
транзакции: object_type = `TRANSACTION`;  
курсы валют: object_type = `CURRENCY`.  

