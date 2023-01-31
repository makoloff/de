INSERT INTO UNKNOWNPAVELYANDEXRU__DWH.l_user_group_activity(
  hk_l_user_group_activity, hk_user_id, 
  hk_group_id, load_dt, load_src
) 
select 
  distinct hash(hu.hk_user_id, hg.hk_group_id) as hk_l_user_group_activity, 
  hu.hk_user_id as hk_user_id, 
  hg.hk_group_id as hk_group_id, 
  now() as load_dt, 
  's3' as load_src 
from 
  UNKNOWNPAVELYANDEXRU__STAGING.group_log as g 
  left join UNKNOWNPAVELYANDEXRU__DWH.h_users as hu on g.user_id = hu.user_id 
  left join UNKNOWNPAVELYANDEXRU__DWH.h_groups as hg on g.group_id = hg.group_id 
where 
  hash(hu.hk_user_id, hg.hk_group_id) not in (
    select 
      hk_l_user_group_activity 
    from 
      UNKNOWNPAVELYANDEXRU__DWH.l_user_group_activity
  );
