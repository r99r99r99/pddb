--得到站点下的能接受短信的人员的信息
CREATE OR REPLACE VIEW view_sms_station_user AS
select c.station_id as stationId,f.title as stationName,
   a.id as userid,a.realname as userName,a.telephone
from sys_user a,sys_role b,sys_role_station c,sys_role_user d,sys_sms_station_user e,aiot_watch_point f
where a.id = d.user_id and d.role_id = b.id
and b.isactive = 1 and b.type = 2
and b.id = c.role_id 
and a.id = e.userid and c.station_id = e.stationid 
and c.station_id = f.id and f.isactive = 1