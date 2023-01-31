CREATE TABLE if not exists dds.dm_api_restaurants (
    id serial4 NOT NULL,
    restaurant_id varchar NOT NULL,
    restaurant_name varchar NOT NULL,
    active_from timestamp NOT NULL,
    active_to timestamp NOT NULL
    ,CONSTRAINT dm_api_restaurants_pkey PRIMARY KEY (id)
    ,CONSTRAINT dm_api_restaurants_restaurant_id_unique UNIQUE (restaurant_id)
);

CREATE TABLE if not exists dds.dm_api_couriers (
    id serial4 NOT NULL,
    courier_id varchar NOT NULL,
    courier_name varchar NOT NULL,
    active_from timestamp NOT NULL,
    active_to timestamp NOT NULL
    ,CONSTRAINT dm_api_couriers_pkey PRIMARY KEY (id)
    ,CONSTRAINT dm_api_restaurants_courier_id_unique UNIQUE (courier_id)

);

CREATE TABLE if not exists dds.dm_api_deliveries (
    id serial4 NOT NULL,
    update_ts timestamp not null,
    order_id varchar NOT NULL,
    order_ts timestamp NOT NULL,
    delivery_id varchar NOT NULL,
    courier_id varchar NOT NULL
    ,address varchar NOT NULL
    ,delivery_ts timestamp NOT NULL
    ,rate smallint not NUll default 1
    ,order_sum numeric(14, 2) not null default 0,
    tip_sum numeric(14, 2) not null default 0

    ,CONSTRAINT dm_api_deliveries_pkey PRIMARY KEY (id)
    ,CONSTRAINT dm_api_deliveries_order_sum_check CHECK ((order_sum >= 0 ))
    ,CONSTRAINT dm_api_deliveries_tip_sum_check CHECK ((tip_sum >= 0 ))
    ,CONSTRAINT dm_api_deliveries_rate_check CHECK ( (rate >= 1 ) and (rate <= 5) )
    ,CONSTRAINT dm_api_deliveries_order_id_unique UNIQUE (order_id)
);


