select * from stg.order_events limit 10;

select count(*) from stg.order_events ;


------------------- хабы ---------------------
select count(*) from dds.h_restaurant limit 10;

select count(*) from dds.h_user limit 20 ;

select count(*) from dds.h_order limit 10 ;

select count(*) from dds.h_product limit 10  ;

select count(*) from dds.h_category limit 10 ;


------------------- линки --------------------------
select count(*) from dds.l_order_product limit 10 ;

select h_order_pk, h_product_pk, count(*) from dds.l_order_product group by 1,2 order by 3 desc limit 5 ;

select count(*) from dds.l_order_user limit 10 ;

select h_order_pk, h_user_pk, count(*) from dds.l_order_user group by 1,2 order by 3 desc limit 5 ;

select count(*) from dds.l_product_category limit 10 ;

select h_product_pk, h_category_pk, count(*) from dds.l_product_category group by 1,2 order by 3 desc limit 5 ;

select count(*) from dds.l_product_restaurant limit 10 ;

select h_product_pk, h_restaurant_pk, count(*) from dds.l_product_restaurant group by 1,2 order by 3 desc limit 5 ;


--------------------- сателлиты ---------------------
select count(*) from dds.s_order_cost limit 10 ;

select * from dds.s_order_cost soc ;

select h_order_pk, count(*) from dds.s_order_cost group by 1 order by 2 desc limit 10 ;

select count(*) from dds.s_order_status limit 10 ;

select * from dds.s_order_status sos ;

select h_order_pk, count(*) from dds.s_order_status group by 1 order by 2 desc limit 10 ;

select count(*) from dds.s_product_names limit 10 ;

select * from dds.s_product_names limit 10 ;

select h_product_pk, count(*) from dds.s_product_names group by 1 order by 2 desc limit 10 ;

select count(*) from dds.s_restaurant_names limit 10 ;

select * from dds.s_restaurant_names limit 10 ;

select h_restaurant_pk, count(*) from dds.s_restaurant_names group by 1 order by 2 desc limit 10 ;

select count(*) from dds.s_user_names limit 10 ;

select h_user_pk, count(*) from dds.s_user_names group by 1 order by 2 desc limit 10 ;

select * from dds.s_user_names limit 10 ;



------------------------ cdm ----------------------------------------
select count(*) from cdm.user_product_counters upc ;

select count(*) from cdm.user_category_counters ucc ;

select * from cdm.user_product_counters where user_id ='66d7c12f-5f93-5fb3-97fe-5db3e70aa6d6'
;
-------------------------------------------------------------------------------------------------
select * from dds.h_user hu where h_user_pk ='66d7c12f-5f93-5fb3-97fe-5db3e70aa6d6'
;

select * from dds.h_product hp where h_product_pk ='0f1b37f0-54df-5b77-b8f5-f8f2fac4c98d';

select * from dds.s_order_cost limit 10 



;
-------------------------------------------------------------------------
select payload, payload::json->>'restaurant' as restaurant
, (payload::json->>'restaurant')::json->'id'
from stg.order_events 
limit 10



