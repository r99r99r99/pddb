CREATE OR REPLACE VIEW view_stationid_deviceid_indicatorid AS
select a.stationid,a.deviceid,a.indicatorid,b.pointNum, c.devicemodel
from aiot_station_device_indicator a,map_awp_device_catalog b,device_catalog c
where a.stationid = b.aiot_watch_point_id
and a.deviceid = b.device_catalog_id
and b.device_catalog_id = c.id and c.id = a.deviceid