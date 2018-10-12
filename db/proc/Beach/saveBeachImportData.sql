## call saveBeachImportData('10004','2018-06-11 08','cc#2,cqj#2,fdcj#2,laji#2,lang#2,oil#2,oilpu#2,pfw#2,qtemp#0,rjy#2,scw#2,see#2,sm#5,tmd#2,weather#2,wind#2,wtemp#33,zao#2',1);
drop procedure if exists saveBeachImportData;
create procedure saveBeachImportData(
	in v_stationId varchar(20),
	in v_colllectTime varchar(20),
	in v_params varchar(300),
	in v_userid int
)
begin
	DECLARE v_result varchar(30);
	##创建保存数据的临时表
	drop table if exists tmp_table;
	CREATE TEMPORARY TABLE tmp_table
	(	 
		name varchar(20),
		val varchar(20)
	);
	
	##将传递的参数保存在临时表中
	set @i = length(v_params) - length(replace(v_params,',',''));  
	set @left_str = v_params;  
	while @i>0                
		  do   
		  set @sub_str = substr(@left_str,1,instr(@left_str,',')-1);              
		  set @left_str = substr(@left_str,length(@sub_str)+length(',')+1);     
		  set @n = trim(@sub_str);  
		  set @name = substring_index(@n,'#',1);
		  set @val = substring_index(@n,'#',-1);
		  if @val!= '-1' then 
			  insert into tmp_table(name,val) values(@name,@val); 
		  end if;
		  set @i = @i - 1;        
	end while;  
	
	insert into aiot_beach_data(stationid,collecttime,userid,code,data)
	select v_stationId,v_colllectTime,v_userid,name,val
	from tmp_table
	on duplicate key update data=values(data);
	set v_result = '保存成功';
	
	select '保存成功' as result;
end ;