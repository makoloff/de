CREATE TABLE if not exists UNKNOWNPAVELYANDEXRU__STAGING.group_log (
  group_id int not null, 
  user_id int not null, 
  user_id_from int, 
  event varchar(10), 
  dt datetime
) 
order by 
  group_id, 
  user_id PARTITION BY datetime :: date 
GROUP BY 
  calendar_hierarchy_day(datetime :: date, 3, 2);