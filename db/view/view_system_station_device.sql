##获得站点下,有展示权限的系统设备的列表
CREATE OR REPLACE VIEW view_system_station_device AS
select distinct c.stationid,a.id,a.name,a.code,a.devicemodel,a.brief,a.detail,a.ordercode
from device_catalog a,map_awp_device_catalog b,aiot_station_device_indicator c
where a.id = b.device_catalog_id
and a.id = c.deviceid and c.stationid = b.aiot_watch_point_id 
and c.deviceid = b.device_catalog_id and c.indicatorid<> 0 
and devicemodel = 'system'