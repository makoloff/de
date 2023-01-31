CREATE TABLE if not exists stg.api_restaurants (
    id serial NOT null, --GENERATED ALWAYS AS IDENTITY,
    object_id varchar NOT NULL,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
    ,CONSTRAINT api_restaurants_pkey PRIMARY KEY (id)
    ,CONSTRAINT api_restaurants_object_id_uindex UNIQUE (object_id)
);



CREATE TABLE if not exists stg.api_couriers (
    id serial NOT null, --GENERATED ALWAYS AS IDENTITY,
    object_id varchar NOT NULL,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
    ,CONSTRAINT api_couriers_pkey PRIMARY KEY (id)
    ,CONSTRAINT api_couriers_object_id_uindex UNIQUE (object_id)
);

CREATE TABLE if not exists stg.api_deliveries (
    id serial NOT null, --GENERATED ALWAYS AS IDENTITY,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
    ,CONSTRAINT api_deliveries_pkey PRIMARY KEY (id)
    ,CONSTRAINT api_deliveries_object_value_uindex UNIQUE (object_value)
);

CREATE TABLE if not exists stg.api_deliveries_temp (
                        --update_ts timestamp not null,
                        order_id varchar NOT NULL,
                        order_ts timestamp NOT NULL,
                        delivery_id varchar NOT NULL,
                        courier_id varchar NOT NULL
                        ,address varchar NOT NULL
                        ,delivery_ts timestamp NOT NULL
                        ,rate smallint not NUll
                        ,order_sum numeric(14, 2) not null default 0,
                        tip_sum numeric(14, 2) not null default 0
                    )

