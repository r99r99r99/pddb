## call getRows4DataQuery(10011,21,'2018-06-13 08:27:51','2018-08-13 08:27:51',0);
drop procedure if exists getRows4DataQuery;
create procedure getRows4DataQuery(
	in v_stationid	int,    ##站点编码
	in v_deviceid		int,  ##设备编码
	in v_begintime varchar(20),  ##查询开始时间
	in v_endtime varchar(20),   ##查询结束时间
	in v_queryType int    ##是否显示源数据  0表示不显示元数据,显示处理过的数据 1表示显示元数据
)
begin
	DECLARE no_more_tables INT DEFAULT 0;  
	DECLARE i_tableName varchar(300);  ##定义要查询的表的名
	DECLARE i_indicatorcode varchar(30);
	DECLARE i_pointnum int default 0;
	DECLARE v_insql varchar(5000);
	DECLARE i_first int default 0;
	
	
	##报警码信息
	DECLARE I_ALARMDATA DOUBLE;
	DECLARE I_BEGINTIME VARCHAR(20);
	DECLARE I_ENDTIME VARCHAR(20);
	DECLARE I_ALARMNAME VARCHAR(40);
	
	##定义插入临时表时的SQL语句
	DECLARE IN_SQL VARCHAR(3000);
	DECLARE IN_INSERTSQL VARCHAR(100);
	DECLARE IN_COLLECTTIMESQL VARCHAR(100) DEFAULT ' SELECT COLLECT_TIME AS COLLECTTIME,';
	DECLARE IN_SELECTSQL VARCHAR(3000) DEFAULT ' case when 1=0 then ''0''';
	DECLARE IN_SELECTSQL_END VARChar(3000);
	DECLARE IN_SELECTSQL_AS VARChar(50);
	DECLARE IN_FROMSQL VARCHAR(50) DEFAULT ' FROM tablename';
	DECLARE IN_WHERESQL VARCHAR(1000) DEFAULT '';
	DECLARE IN_WHERESQL_REPLACE VARCHAR(100);
	DECLARE IN_CONCAT_BEFORE VARCHAR(20) DEFAULT '';
	DECLARE IN_CONCAT_AFTER VARCHAR(200) DEFAULT '';
	DECLARE IN_CONCAT_ACTIVE VARCHAR(200) DEFAULT '';
	
	##定义监测参数的量程
	DECLARE IN_MINDATA DOUBLE;
	DECLARE IN_MAXDATA DOUBLE;
	DECLARE IN_RANGESQL VARCHAR(1000) DEFAULT '';
	
	##查询该站点需要查询的表
	DECLARE CUR1 CURSOR FOR 
		SELECT TABLENAME
		FROM aiot_meta_table  
		WHERE wpid = v_stationid AND TYPE = 1 
		AND BEGINTIME <= v_endtime and endtime > v_begintime;
	##查询该站点 该设备检测的参数集合
	DECLARE CUR2 CURSOR FOR
		SELECT INDICATORCODE,pointnum
		FROM view_stationid_deviceid_indicatorcode a
		WHERE stationid = v_stationId
		AND DEVICEID = v_deviceid;
	##读取设备的报警码信息
	DECLARE CUR3 CURSOR FOR
		SELECT ALARMDATA,BEGINTIME,ENDTIME,B.TITLE AS ALARMNAME
		FROM aiot_device_alarm A,aiot_device_alarm_config B
		WHERE A.CONFIGID = B.ID
		AND A.STATIONID = V_STATIONID
		AND A.DEVICEID = V_DEVICEID;
	##查询站点 设备 参数的量程范围
	DECLARE CUR4 CURSOR FOR
		select mindata,maxdata
		from aiot_range_data 
		where stationid = v_stationid
		and deviceid = v_deviceid
		and indicatorcode = i_indicatorcode;
				
				
	DECLARE CONTINUE HANDLER FOR NOT FOUND  SET  no_more_tables = 1;
	
	## 定义最终查询的临时表
	drop table if exists tmp_table;
	CREATE TEMPORARY TABLE tmp_table
	(	 
		collectTime		varchar(20) not null primary key
	);
	
	##根据传入条件,得到where语句
	SET IN_WHERESQL = CONCAT(IN_WHERESQL,' WHERE WPID = ',V_STATIONID);
	SET IN_WHERESQL = CONCAT(IN_WHERESQL,' and deviceid = ',v_deviceid);
	SET IN_WHERESQL = CONCAT(IN_WHERESQL,' and indicator_code = ''replace_indicator''');
	SET IN_WHERESQL = CONCAT(IN_WHERESQL,' and collect_time >=''',v_begintime,'''');
	SET IN_WHERESQL = CONCAT(IN_WHERESQL,' and collect_time <=''',v_endtime,'''');
	
	if v_queryType = 0 then 
		##遍历设备的报警码信息
		set no_more_tables = 0;
		OPEN CUR3;
		REPEAT
		FETCH CUR3 INTO I_ALARMDATA,I_BEGINTIME,I_ENDTIME,I_ALARMNAME;
			if not no_more_tables then
				SET IN_SELECTSQL = CONCAT(IN_SELECTSQL,' WHEN DATA = ',I_ALARMDATA)	 ;   		
				SET IN_SELECTSQL = CONCAT(IN_SELECTSQL,' AND COLLECT_TIME >=''',I_BEGINTIME,'''')	 ;   		
				SET IN_SELECTSQL = CONCAT(IN_SELECTSQL,' AND COLLECT_TIME <=''',I_ENDTIME,'''')	 ;   		
				SET IN_SELECTSQL = CONCAT(IN_SELECTSQL,' THEN ''',I_ALARMNAME,'''')	 ;   		
			end if;
	    UNTIL  no_more_tables = 1  END REPEAT;
	    close CUR3;
	    
	    SET IN_CONCAT_BEFORE = ' CONCAT(';
	    SET IN_CONCAT_ACTIVE = ',CASE ISACTIVE WHEN 0 THEN ''(无效数据)'' WHEN 1 THEN '''' ELSE ''(无效状态)'' END';
	    SET IN_CONCAT_AFTER = ',CASE WHEN LENGTH(REMARK)>0 THEN CONCAT(''['',REMARK,'']'') ELSE '''' END )';
	end if;
	
	##得到该站点 设备检测的参数集合
	set no_more_tables = 0;
	OPEN CUR2;
	REPEAT
    FETCH CUR2 INTO i_indicatorcode,i_pointnum;
    	if not no_more_tables then
    	
    		SET IN_RANGESQL = '';
    		
	    	SET IN_SQL = concat('insert into tmp_table(collectTime,',i_indicatorcode,')');
	    	
    		SET v_insql = 'ALTER TABLE tmp_table ADD COLUMN `';
    		SET v_insql = CONCAT(v_insql,i_indicatorcode,'` varchar(255) null');
    		set @v_sql=v_insql;
			prepare stmt from @v_sql;
			EXECUTE stmt;
			deallocate prepare stmt; 
			
			
			if v_queryType = 0 then 
				OPEN cur4;
				REPEAT
			    FETCH cur4 INTO IN_MINDATA,IN_MAXDATA;
			    	if not no_more_tables then
			    		SET IN_RANGESQL = CONCAT(IN_RANGESQL,' WHEN DATA <',IN_MINDATA,' THEN ''<',IN_MINDATA,'''');
			    		SET IN_RANGESQL = CONCAT(IN_RANGESQL,' WHEN DATA >',IN_MAXDATA,' THEN ''>',IN_MAXDATA,'''');
				    end if;
			    UNTIL  no_more_tables = 1  END REPEAT;
			    close cur4;
			    set no_more_tables = 0;
			end if;
			
			SET IN_SELECTSQL_END = concat(' else cast( ROUND(data,',i_pointnum,') as char) end ');
			
			SET IN_SELECTSQL_AS = CONCAT(' AS ',i_indicatorcode);
			##遍历要查询的表
			set i_first = 0;
			OPEN cur1;
			REPEAT
		    FETCH cur1 INTO i_tableName;
		    	if not no_more_tables then
		    		if i_first >0 then
		    			SET IN_SQL = concat(IN_SQL,' union all ');
		    		end if;
		    		SET IN_SQL = concat(IN_SQL,IN_COLLECTTIMESQL,IN_CONCAT_BEFORE,IN_SELECTSQL,IN_RANGESQL,IN_SELECTSQL_END,IN_CONCAT_ACTIVE,IN_CONCAT_AFTER,IN_SELECTSQL_AS);
		    		SET IN_SQL = concat(IN_SQL,replace(IN_FROMSQL,'tablename',i_tablename));
		    		SET IN_SQL = concat(IN_SQL,replace(IN_WHERESQL,'replace_indicator',i_indicatorcode));
		    		
		    		set i_first = i_first + 1;
			    end if;
		    UNTIL  no_more_tables = 1  END REPEAT;
		    close cur1;
		    set no_more_tables = 0;
	    	set IN_SQL = concat(IN_SQL,' on duplicate key update ',i_indicatorcode,'=values(',i_indicatorcode,')');
			##执行当前语句的插入工作
			set @v_sql=in_sql;
			prepare stmt from @v_sql;
			EXECUTE stmt;
			deallocate prepare stmt; 
	    end if;
    	UNTIL  no_more_tables = 1  END REPEAT;
    close CUR2;
	
    select * from tmp_table ORDER BY COLLECTTIME DESC;
end;