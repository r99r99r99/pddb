## call getLastBeachDegreeByStation(10004);
drop procedure if exists getLastBeachDegreeByStation;
create procedure getLastBeachDegreeByStation(
	in v_stationId int
)
leave_name:
begin
	DECLARE no_more_tables INT DEFAULT 0;  
	DECLARE i_code varchar(30);
	DECLARE i_name varchar(50);
	DECLARE i_groupid int;
	DECLARE i_groupname varchar(30);
	DECLARE i_type int;
	DECLARE i_data double;
	DECLARE i_dataValue varchar(200);
	DECLARE i_collecttime varchar(30);
	DECLARE i_badreason varchar(50);
	DECLARE i_levelid int;
	DECLARE i_levelname varchar(30);
	DECLARE i_unit varchar(30);
	
	##参数指向
	DECLARE i_deviceid int;
	DECLARE i_indicatorcode varchar(20);
	DECLARE i_tounit double;
	
	##在线监测系统中读取数据的表名
	DECLARE i_tablename varchar(50);
	DECLARE i_currenttime timestamp DEFAULT CURRENT_TIMESTAMP;
	
	DECLARE v_sql varchar(300);
	
	##遍历所有编码
	DECLARE CUR1 CURSOR FOR
	select a.code,a.name,a.type,a.unit,a.groupid,b.name as groupname
	from aiot_beach_code a,aiot_beach_group b
	where a.groupid = b.id;
	DECLARE CONTINUE HANDLER FOR sqlstate '02000' SET  no_more_tables = 1;
	
	##创建保存所有参数最终数据的临时表
	drop table if exists tmp_datatable;
	CREATE TEMPORARY TABLE tmp_datatable
	(	 
		code varchar(30),
		name varchar(50),
		groupid int,
		groupname varchar(30),
		type int,
		data double,
		dataValue varchar(200),
		collecttime varchar(30),
		badreason varchar(50),
		levelid int,
		levelname varchar(30)
	);
	
	##根据当前站点以及查询时间获得需要查询的表名
	SELECT TABLENAME INTO i_tablename
	FROM AIOT_META_TABLE
	WHERE TYPE = 1 AND WPID = V_STATIONID
	AND ISACTIVE = 1 
	AND BEGINTIME <= i_currenttime AND ENDTIME >= i_currenttime
	LIMIT 1;
	
	OPEN cur1;
	REPEAT
    FETCH cur1 INTO i_code,i_name,i_type,i_unit,i_groupid,i_groupname;
    	if not no_more_tables then
    		 select i_code,i_type;
    		
    		if i_unit is null or i_unit = '-' then 
    			set i_unit = ' ';
    		end if;
    		##得到该参数在上报数据表中的数据
    		if i_type = 1 then
    			select a.collecttime,a.data,concat(a.data,i_unit)
    			into i_collecttime,i_data,i_datavalue
    			from aiot_beach_data a
				where a.stationid = v_stationid
				and code = i_code
				order by collecttime desc
				limit 1;
    		else 
    			select a.collecttime,a.data,b.value as datavalue
    			into i_collecttime,i_data,i_datavalue
				from aiot_beach_data a,aiot_beach_config b
				where a.data = b.data
				and a.code = b.code
				and a.code = i_code
				and a.stationid = v_stationid
				order by a.collecttime desc
				limit 1;
    		end if;
    		
    		set no_more_tables = 0;  ##避免以上查询数据为空时,跳出整个循环
    		if i_type = 1 then
    			##判断该参数是否有指向target  自动读取数据
    			select deviceid,indicatorcode,tounit
    			into i_deviceid,i_indicatorcode,i_tounit
    			from aiot_beach_target
    			where stationid = v_stationid
    			and code = i_code;
    			set no_more_tables = 0;  ##避免以上查询数据为空时,跳出整个循环
    			
    			##当有指向时,读取该参数在系统中的最后一条数据
    			if i_deviceid is not null and i_indicatorcode is not null and i_tounit is not null then
    				IF i_tablename IS NOT NULL THEN 
    				
						set v_sql = '';
						set v_sql = concat(v_sql,' set i_data = ');
						set v_sql = concat(v_sql,' (SELECT DATA*',i_tounit);
						set v_sql = concat(v_sql,' FROM ',i_tablename);
						set v_sql = concat(v_sql,' WHERE WPID =',V_STATIONID);
						set v_sql = concat(v_sql,' AND INDICATOR_CODE = ''',V_INDICATORCODE,'''');
						set v_sql = concat(v_sql,' AND COLLECT_TIME >=''',i_collecttime,'''');
						set v_sql = concat(v_sql,' ORDER BY COLLECT_TIME DESC LIMIT 1 )');
						select v_sql;
						set @v_sql=v_sql;
						prepare stmt from @v_sql;
						EXECUTE stmt;
		 				deallocate prepare stmt; 
		 				
		 				SELECT CONCAT('I-DATA改变后为',i_data);
					END IF;
    				
    			end if;
    			
    		end if;
    	end if;
    UNTIL  no_more_tables = 1  END REPEAT;
    close cur1;
	
end ;