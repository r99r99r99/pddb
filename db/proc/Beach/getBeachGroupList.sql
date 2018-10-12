## call getBeachGroupList();
drop procedure if exists getBeachGroupList;
create procedure getBeachGroupList(
)
begin
	select id,name,ordercode
	from aiot_beach_group
	order by ordercode ;
end ;