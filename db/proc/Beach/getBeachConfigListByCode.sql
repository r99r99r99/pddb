## call getBeachConfigListByCode('sm');
drop procedure if exists getBeachConfigListByCode;
create procedure getBeachConfigListByCode(
	in v_code varchar(20)
)
begin
	select id,code,data,value
	from aiot_beach_config
	where code = v_code;
end ;