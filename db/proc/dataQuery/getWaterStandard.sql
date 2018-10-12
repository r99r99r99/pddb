## call getWaterStandard(10004);
drop procedure if exists getWaterStandard;
create procedure getWaterStandard(
	in v_stationid	int    ##站点编码
)
begin
	DECLARE no_more_tables INT DEFAULT 0;  
	DECLARE i_now varchar(30);  ##定义当前的时间
	DECLARE i_watertype int;  ##获得站点的水质类型
	DECLARE i_tableName varchar(300);  ##定义要查询的表的名
	declare v_insql varchar(5000);  ##定义插入元数据的sql
	DECLARE i_count int;  
	
	DECLARE i_standard_grade int ;
	DECLARE i_standard_name varchar(20) ;
	DECLARE i_indicatorcode varchar(200) ;
	DECLARE i_indicatorname varchar(200) ;
	DECLARE i_data double;
	DECLARE i_unit varchar(20) ;
	
	##初始化返回结果
	DECLARE i_grade int DEFAULT 0;
	
	##查询该站点需要查询的表
	DECLARE CUR1 CURSOR FOR 
	select indicatorcode,indicatorname,standard_grade,standard_name,data,unit
	from tmp_aiot;
	
	DECLARE CONTINUE HANDLER FOR NOT FOUND  SET  no_more_tables = 1;
	
	drop table if exists tmp_aiot;   ##通过该表获得参数的最后一条数据
	CREATE TEMPORARY TABLE tmp_aiot
	(	 
		collectTime		varchar(20),
		indicatorcode	varchar(30),
		indicatorname	varchar(30),
		data			double default 0,
		unit 			varchar(30),
		standard_grade  int,
		standard_name   varchar(30)
	);
	
	drop table if exists tmp_result;   ##通过该表获得参数的最后一条数据
	CREATE TEMPORARY TABLE tmp_result
	(	 
		standard_grade		int,
		standardName			varchar(30),
		standValue		varchar(300)
	);
	
	insert into tmp_result(standard_grade,standardname,standvalue)
	values(0,'','');
	
	##获得当前站点的水质类型
	select watertype into i_watertype
	from aiot_watch_point 
	where id = v_stationid;
	
	##获得当前的时间
	select now() into i_now;
	##根据当前时间获得需要查询的表
	select a.tablename into i_tablename
	from aiot_meta_table a
	where wpid = v_stationid
	and begintime <= i_now
	and endtime > i_now
	and type = 1 and isactive = 1;
	
	
	## 判断设备报警编码表是否为空表   注:如果是空表,不参与关联,否则会导致查询为空
	select count(1) into i_count from aiot_device_alarm;
	set v_insql = ' insert into tmp_aiot(collecttime,indicatorcode,indicatorname,data,standard_grade,standard_name,unit)';
	set v_insql = concat(v_insql,' select max(k.collect_time),k.indicator_code,k.indicatorname,k.data,k.standard_grade,k.standard_name,k.unit');
	set v_insql = concat(v_insql,' from ( ');
	set v_insql = concat(v_insql,' select a.collect_time,a.indicator_code,b.title as indicatorname,a.data,c.logo as unit,m.standard_grade,n.classname as standard_name');
	set v_insql = concat(v_insql,' from ',i_tablename,' a ,dm_indicator b,g_unit c,waterqualitystandard m, g_waterstandard_config n');
	if i_count >0 then 
		set v_insql = concat(v_insql,',aiot_device_alarm d');   ##去除设备报警数据
	end if;
	set v_insql = concat(v_insql,' where a.indicator_code = b.code and b.unitid = c.id');
	set v_insql = concat(v_insql,' and a.isactive = 1');   ##去除手动标注异常数据
	set v_insql = concat(v_insql,' and b.isactive = 1');
	if i_count >0 then 
	set v_insql = concat(v_insql,' and !(a.wpid = d.stationid and a.deviceid = d.deviceid and a.data = d.alarmdata)');   ##去除设备报警数据
	end if;
	set v_insql = concat(v_insql,' and a.indicator_code = m.item and m.water_type = ',i_watertype);
	set v_insql = concat(v_insql,' and case when m.min=1 then a.data >= m.min_value else a.data > m.min_value end');
	set v_insql = concat(v_insql,' and case when m.max=1 then a.data <= m.max_value else a.data < m.max_value end ');
	set v_insql = concat(v_insql,' and n.typeid = ',i_watertype,' and m.standard_grade = n.classid');
	set v_insql = concat(v_insql,' order by a.collect_time desc');
	set v_insql = concat(v_insql,' ) k group by k.indicator_code');
	
	set @v_sql=v_insql;
	prepare stmt from @v_sql;
	EXECUTE stmt;
	deallocate prepare stmt; 
	
	set no_more_tables = 0;	 	
	OPEN CUR1;
		REPEAT
		FETCH CUR1 INTO  i_indicatorcode,i_indicatorname,i_standard_grade,i_standard_name,i_data,i_unit;
			if not no_more_tables then
				 	if i_standard_grade > i_grade then
				 		set i_grade = i_standard_grade;
				 		update tmp_result set 
				 		standard_grade = i_standard_grade,
				 		standardName = i_standard_name,
				 		standValue = i_indicatorname;
				 	ELSEIF i_standard_grade = i_grade then
				 		update tmp_result set 
				 		standValue = concat(standValue,',',i_indicatorname);
				 	end if;
			end if;
	    UNTIL  no_more_tables = 1  END REPEAT;
	    close CUR1;	
	    
	 update tmp_result set standValue = '水质良好' where  standard_grade<=2;
	 update tmp_result set standValue = concat('当前水质因子为',standValue) where  standard_grade>2;
	 
	 select * from tmp_result;
end;