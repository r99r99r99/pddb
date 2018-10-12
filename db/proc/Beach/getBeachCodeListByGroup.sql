## call getBeachCodeListByGroup(1);
drop procedure if exists getBeachCodeListByGroup;
create procedure getBeachCodeListByGroup(
	in v_groupid int
)
begin
	select id,groupId,code,name,type,unit,ordercode
	from aiot_beach_code
	where groupid = v_groupid
	order by ordercode;
end ;