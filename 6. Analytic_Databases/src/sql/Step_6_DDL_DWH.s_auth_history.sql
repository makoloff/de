CREATE TABLE if not exists UNKNOWNPAVELYANDEXRU__DWH.s_auth_history (
  hk_l_user_group_activity bigint primary key, 
  user_id_from int, 
  event varchar(10) not null, 
  event_dt timestamp, 
  load_dt datetime, 
  load_src varchar(20)
) 
order by 
  load_dt segmented by hk_l_user_group_activity all nodes PARTITION BY event_dt :: date 
GROUP BY 
  calendar_hierarchy_day(event_dt :: date, 3, 2);
