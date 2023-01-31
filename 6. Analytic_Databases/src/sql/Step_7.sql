----------------------- STEP 7 ---------------------
with groups as (
  SELECT 
    hk_group_id 
  FROM 
    UNKNOWNPAVELYANDEXRU__DWH.h_groups 
  order by 
    registration_dt 
  limit 
    10
), 

added as (
  SELECT 
    hk_group_id, 
    count(distinct g.hk_user_id) as cnt_added_users 
  FROM 
    UNKNOWNPAVELYANDEXRU__DWH.l_user_group_activity g 
  where 
    g.hk_l_user_group_activity in (
      SELECT 
        hk_l_user_group_activity 
      FROM 
        UNKNOWNPAVELYANDEXRU__DWH.s_auth_history 
      where 
        event = 'add'
    ) 
    and g.hk_group_id in (
      SELECT 
        hk_group_id groups
    ) 
  group by 
    1
), 

wrote_1 as (
  select 
    * 
  from 
    UNKNOWNPAVELYANDEXRU__DWH.l_groups_dialogs 
  where 
    hk_group_id in (
      select 
        hk_group_id 
      from 
        groups
    )
), 

wrote_2 as (
  select 
    w.hk_group_id, 
    count(distinct m.hk_user_id) as cnt_users_in_group_with_messages 
  from 
    wrote_1 w 
    left join UNKNOWNPAVELYANDEXRU__DWH.l_user_message m on m.hk_message_id = w.hk_message_id 
  group by 
    1
) 

select 
  a.*, 
  w.cnt_users_in_group_with_messages, 
  (
    w.cnt_users_in_group_with_messages / a.cnt_added_users
  ):: numeric(14, 2) as group_conversion 
from 
  added a 
  left join wrote_2 w on w.hk_group_id = a.hk_group_id 
order by 
  4 desc
