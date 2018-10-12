--根据人员选出当前人员权限下的站点的列表
CREATE OR REPLACE VIEW view_role_user_station AS
select d.user_id as userid,a.id,a.code,a.title,a.latitude,a.longitude,a.station_gateway,
a.stationtype_id,a.region_id,a.companyid,a.brief,a.detail,a.pic,a.isactive,a.watertype,a.ordercode
from aiot_watch_point a,sys_role_station b,sys_role c,sys_role_user d
where a.id = b.station_id and b.role_id = c.id and c.isactive = 1 and a.isactive = 1
and c.id = d.role_id 