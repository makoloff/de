CREATE SCHEMA IF NOT EXISTS cdm ;


CREATE TABLE if not exists cdm.user_product_counters (
id serial NOT null, --GENERATED ALWAYS AS IDENTITY,
    user_id UUID NOT NULL,
    product_id UUID NOT NULL,
    product_name varchar NOT null
    ,order_cnt int not null
    ,CONSTRAINT user_product_counters_pkey PRIMARY KEY (id)
    ,constraint user_product_counters_cnt CHECK ((order_cnt >= 0 ))
    ,CONSTRAINT user_product_counters_uindex UNIQUE (user_id, product_id)
);

select * from cdm.user_product_counters  ;

CREATE TABLE if not exists cdm.user_category_counters (
id serial NOT null, --GENERATED ALWAYS AS IDENTITY,
    user_id UUID NOT NULL,
    category_id UUID NOT NULL,
    category_name varchar NOT null
    ,order_cnt int not null
    ,CONSTRAINT user_category_counters_pkey PRIMARY KEY (id)
    ,constraint user_category_counters_cnt CHECK ((order_cnt >= 0 ))
    ,CONSTRAINT user_category_counters_uindex UNIQUE (user_id, category_id)
);

select * from cdm.user_category_counters ;


------------------------------ STG LAYER -------------------------------------------------------

create schema if not exists stg ;

CREATE TABLE if not exists stg.order_events (
id serial NOT null, --GENERATED ALWAYS AS IDENTITY,
object_id int NOT NULL,
object_type varchar NOT NULL,
sent_dttm timestamp NOT null
,payload  json not null
,CONSTRAINT order_events_pkey PRIMARY KEY (id)
,CONSTRAINT order_events_uindex UNIQUE (object_id)
);

select * from stg.order_events ;


------------------------------- DDS LAYER ----------------------------------------------------------

CREATE TABLE if not exists dds.h_user (
h_user_pk UUID NOT null, --GENERATED ALWAYS AS IDENTITY,
user_id varchar NOT NULL,
load_dt timestamp NOT null
,load_src  varchar not null
,CONSTRAINT h_user_pkey PRIMARY KEY (h_user_pk)
);

select * from dds.h_user ; 


CREATE TABLE if not exists dds.h_product  (
h_product_pk UUID NOT null, --GENERATED ALWAYS AS IDENTITY,
product_id varchar NOT NULL,
load_dt timestamp NOT null
,load_src  varchar not null
,CONSTRAINT h_product_pkey PRIMARY KEY (h_product_pk)
);

select * from dds.h_product ; 


CREATE TABLE if not exists dds.h_category  (
h_category_pk UUID NOT null, --GENERATED ALWAYS AS IDENTITY,
category_name varchar NOT NULL,
load_dt timestamp NOT null
,load_src  varchar not null
,CONSTRAINT h_category_pk_pkey PRIMARY KEY (h_category_pk)
);

select * from dds.h_category ; 



CREATE TABLE if not exists dds.h_restaurant   (
h_restaurant_pk UUID NOT null, --GENERATED ALWAYS AS IDENTITY,
restaurant_id varchar NOT NULL,
load_dt timestamp NOT null
,load_src  varchar not null
,CONSTRAINT h_restaurant_pk_pkey PRIMARY KEY (h_restaurant_pk)
);

select * from dds.h_restaurant ; 




CREATE TABLE if not exists dds.h_order (
h_order_pk UUID NOT null, --GENERATED ALWAYS AS IDENTITY,
order_id int NOT NULL,
order_dt timestamp not null,
load_dt timestamp NOT null
,load_src  varchar not null
,CONSTRAINT h_order_pkey PRIMARY KEY (h_order_pk)
);

select * from dds.h_order ; 


------------------------- СОЗДАНИЕ ТАБЛИЦ - ЛИНКОВ ---------------------------------------------------


CREATE TABLE if not exists dds.l_order_product (
hk_order_product_pk UUID NOT null,
h_order_pk UUID NOT null, -- ссылка на 1ю таблицу хаба
h_product_pk UUID NOT null, -- ссылка на 2ю табл хаба
load_dt timestamp NOT null
,load_src  varchar not null

,CONSTRAINT l_order_product_pkey PRIMARY KEY (hk_order_product_pk)
,CONSTRAINT h_order_pk_fk FOREIGN key (h_order_pk) REFERENCES dds.h_order(h_order_pk) ON DELETE SET NULL
,CONSTRAINT h_product_pk_fk FOREIGN key (h_product_pk) REFERENCES dds.h_product(h_product_pk) ON DELETE SET NULL
);

select * from dds.l_order_product ;




CREATE TABLE if not exists dds.l_product_restaurant (
hk_product_restaurant_pk UUID NOT null,
h_product_pk UUID NOT null, -- ссылка на 1ю таблицу хаба
h_restaurant_pk UUID NOT null, -- ссылка на 2ю табл хаба
load_dt timestamp NOT null
,load_src  varchar not null

,CONSTRAINT l_product_restaurant_pkey PRIMARY KEY (hk_product_restaurant_pk)
,CONSTRAINT h_product_pk_fk FOREIGN key (h_product_pk) REFERENCES dds.h_product(h_product_pk) ON DELETE SET NULL
,CONSTRAINT h_restaurant_pk_fk FOREIGN key (h_restaurant_pk) REFERENCES dds.h_restaurant(h_restaurant_pk) ON DELETE SET NULL
);

select * from dds.l_product_restaurant ; 



CREATE TABLE if not exists dds.l_product_category (
hk_product_category_pk UUID NOT null,
h_product_pk UUID NOT null, -- ссылка на 1ю таблицу хаба
h_category_pk UUID NOT null, -- ссылка на 2ю табл хаба
load_dt timestamp NOT null
,load_src  varchar not null

,CONSTRAINT l_product_category_pkey PRIMARY KEY (hk_product_category_pk)
,CONSTRAINT h_product_pk_fk FOREIGN key (h_product_pk) REFERENCES dds.h_product(h_product_pk) ON DELETE SET NULL
,CONSTRAINT h_category_pk_fk FOREIGN key (h_category_pk) REFERENCES dds.h_category(h_category_pk) ON DELETE SET NULL
);

select * from dds.l_product_category ; 



CREATE TABLE if not exists dds.l_order_user (
hk_order_user_pk UUID NOT null,
h_order_pk UUID NOT null, -- ссылка на 1ю таблицу хаба
h_user_pk UUID NOT null, -- ссылка на 2ю табл хаба
load_dt timestamp NOT null
,load_src  varchar not null

,CONSTRAINT l_order_user_pkey PRIMARY KEY (hk_order_user_pk)
,CONSTRAINT h_order_pk_fk FOREIGN key (h_order_pk) REFERENCES dds.h_order(h_order_pk) ON DELETE SET NULL
,CONSTRAINT h_user_pk_fk FOREIGN key (h_user_pk) REFERENCES dds.h_user(h_user_pk) ON DELETE SET NULL
);

select * from dds.l_order_user ; 



------------------------------- СОЗДАНИЕ ТАБЛИЦ _ САТЕЛЛИТОВ ------------------------------------------------------------


CREATE TABLE if not exists dds.s_user_names (
hk_user_names_pk UUID NOT null,
h_user_pk UUID NOT null, -- ссылка на таблицу хаба
username varchar NOT null,
userlogin varchar not null,
load_dt timestamp NOT null
,load_src  varchar not null

,constraint s_user_names_pk primary key (hk_user_names_pk)
,CONSTRAINT h_user_pk_fk FOREIGN key (h_user_pk) REFERENCES dds.h_user(h_user_pk) ON DELETE SET NULL
);

select * from dds.s_user_names ; 



CREATE TABLE if not exists dds.s_product_names (
hk_product_names_pk UUID NOT null,
h_product_pk UUID NOT null, -- ссылка на таблицу хаба
name varchar NOT null,
load_dt timestamp NOT null
,load_src  varchar not null

,constraint s_product_names_pk primary key (hk_product_names_pk)
,CONSTRAINT h_product_pk_fk FOREIGN key (h_product_pk) REFERENCES dds.h_product(h_product_pk) ON DELETE SET NULL
);

select * from dds.s_product_names ; 




CREATE TABLE if not exists dds.s_restaurant_names (
hk_restaurant_names_pk UUID NOT null,
h_restaurant_pk UUID NOT null, -- ссылка на таблицу хаба
name varchar NOT null,
load_dt timestamp NOT null
,load_src  varchar not null

,constraint s_restaurant_names_pk primary key (hk_restaurant_names_pk)
,CONSTRAINT h_restaurant_pk_fk FOREIGN key (h_restaurant_pk) REFERENCES dds.h_restaurant(h_restaurant_pk) ON DELETE SET NULL
);

select * from dds.s_restaurant_names ; 





CREATE TABLE if not exists dds.s_order_cost (
hk_order_cost_pk UUID NOT null,
h_order_pk UUID NOT null, -- ссылка на таблицу хаба
cost decimal(19, 5) NOT null,
payment decimal(19, 5) NOT null,
load_dt timestamp NOT null
,load_src  varchar not null

,constraint s_order_cost_pk primary key (hk_order_cost_pk)
,CONSTRAINT h_order_pk_fk FOREIGN key (h_order_pk) REFERENCES dds.h_order(h_order_pk) ON DELETE SET NULL
);

select * from dds.s_order_cost ; 




CREATE TABLE if not exists dds.s_order_status (
hk_order_status_pk UUID NOT null,
h_order_pk UUID NOT null, -- ссылка на таблицу хаба
status varchar NOT null,
load_dt timestamp NOT null
,load_src  varchar not null

,constraint s_order_status_pk primary key (hk_order_status_pk)
,CONSTRAINT h_order_pk_fk FOREIGN key (h_order_pk) REFERENCES dds.h_order(h_order_pk) ON DELETE SET NULL
);

select * from dds.s_order_status ; 
