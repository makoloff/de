COPY UNKNOWNPAVELYANDEXRU__STAGING.group_log (
  group_id, user_id, user_id_from, event, 
  datetime
) 
FROM 
  LOCAL '/data/group_log.csv' DELIMITER ',';
