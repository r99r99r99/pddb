CREATE OR REPLACE VIEW view_stationid_deviceid_indicatorcode AS
select a.stationid,a.deviceid,d.code as indicatorcode,b.pointNum
from aiot_station_device_indicator a,map_awp_device_catalog b,device_catalog c,dm_indicator d
where a.stationid = b.aiot_watch_point_id
and a.indicatorid = d.id
and a.deviceid = b.device_catalog_id
and b.device_catalog_id = c.id and c.id = a.deviceid