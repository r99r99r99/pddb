## call getRowsStatisReportDatas(10002,20,'DO','0,avgdata,mindata,maxdata,diffdata,amplidata',2,'2016-09-13 08:27:51','2017-02-13 08:27:51');
drop procedure if exists getRowsStatisReportDatas;
create procedure getRowsStatisReportDatas(
	in v_stationid	int,
	in v_deviceid		int,
	in v_indicatorcode	varchar(50),
	in v_stattypes varchar(200),
	in v_collectType  int,
	in v_begintime varchar(20),
	in v_endtime varchar(20)
)
begin
	## 变量声明
	DECLARE no_more_tables INT DEFAULT 0;  
	declare stattype varchar(20);
	declare stablename varchar(300);  ##定义要查询的表的名
	declare sbegintime varchar(20);
	declare sendtime varchar(20);
	
	declare i_alarmData double;  
	declare i_beginTime varchar(20);
	declare i_endTime varchar(20);
	
	declare i_mindata double;
	declare i_maxdata double;
	
	##定义sql语句部分
	declare v_insql varchar(5000);
	declare collectSql varchar(200);
	declare collectB varchar(200);
	declare v_sql varchar(5000) default 'insert into tmp_table(collectTime';
	declare selectSql varchar(500) default ' select ';
	declare v_testsql varchar(4000);
	declare alarmSql varchar(1000) default '';
	declare rangeSql varchar(1000) default '';
	
	DECLARE CUR1 CURSOR FOR SELECT TABLENAME,begintime,endtime FROM aiot_meta_table  WHERE wpid = v_stationid AND TYPE = 1 AND BEGINTIME <= v_endtime and endtime > v_begintime;
	##查询站点 设备的报警码信息
	DECLARE CUR2 CURSOR FOR 
	SELECT alarmdata,begintime,endtime
	from aiot_device_alarm
	where stationid = v_stationid
	and deviceid = v_deviceid;
	##查询站点 设备参数的监测量程
	DECLARE CUR3 CURSOR FOR 
	SELECT mindata,maxdata
	from aiot_range_data
	where stationid = v_stationid
	and deviceid = v_deviceid
	and indicatorcode = v_indicatorcode;
		
    DECLARE CONTINUE HANDLER FOR NOT FOUND  SET  no_more_tables = 1;
	drop table if exists tmp_table;
	CREATE TEMPORARY TABLE tmp_table
	(	 
		collectTime		varchar(20) not null primary key,
		avgdata			double default 0,
		maxdata			double default 0,
		maxtime			datetime,
		mindata			double default 0,
		mintime 		datetime,
		diffdata		double default 0,
		amplidata		double default 0
	);
	drop table if exists tmp_aiot;
	CREATE TEMPORARY TABLE tmp_aiot
	(	 
		collectTime		datetime,
		wpid		int,
		deviceid		int,
		indicator_code	varchar(30),
		data			double default 0
	);
	
	## 处理得到统计口径
	case  v_collectType
		when 1 then set collectSql = 'date_format(collecttime,''%Y-%m-%d %H'')',
						collectB = 'date_format(b.collecttime,''%Y-%m-%d %H'')';
		when 2 then set collectSql = 'date_format(collecttime,''%Y-%m-%d'')',
						collectB = 'date_format(b.collecttime,''%Y-%m-%d'')';
		when 3 then set collectSql = 'concat(year(collecttime),case when length(week(collecttime,1)) = 2 then week(collecttime,1) else concat(''0'',week(collecttime,1)) end)',
						collectB = 'concat(year(b.collecttime),case when length(week(b.collecttime,1)) = 2 then week(b.collecttime,1) else concat(''0'',week(b.collecttime,1)) end)';
		when 4 then set collectSql = 'date_format(collecttime,''%Y-%m'')' ,
						collectB = 'date_format(b.collecttime,''%Y-%m'')' ;
		when 5 then set collectSql = 'date_format(collecttime,''%Y'')',
						collectB = 'date_format(b.collecttime,''%Y'')';
	end case;
	
	##查询该站点 设备的设备报警码
	OPEN cur2;
	REPEAT
    FETCH cur2 INTO i_alarmData,i_beginTime,i_endTime;
    	if not no_more_tables then
    		set alarmSql = concat(alarmSql,' and !(');
    		set alarmSql = concat(alarmSql,' data = ',i_alarmData);
    		set alarmSql = concat(alarmSql,' and collect_time >=''',i_beginTime,'''');
    		set alarmSql = concat(alarmSql,' and collect_time <=''',i_endTime,'''');
    		set alarmSql = concat(alarmSql,' )');
	    end if;
    UNTIL  no_more_tables = 1  END REPEAT;
    close cur2;
    
    ##查询监测参数的阈值范围
    set rangeSql = concat(rangeSql,' case when 1=0 then 0 ');
    set no_more_tables = 0;
    OPEN cur3;
	REPEAT
    FETCH cur3 INTO i_mindata,i_maxdata;
    	if not no_more_tables then
    		set rangeSql = concat(rangeSql,' when data<',i_mindata,' then ',i_mindata);
    		set rangeSql = concat(rangeSql,' when data>',i_maxdata,' then ',i_maxdata);
	    end if;
    UNTIL  no_more_tables = 1  END REPEAT;
    close cur3;
    set rangeSql = concat(rangeSql,' else data end as data');
	##将待查询的数据全部保存到临时表中
	set no_more_tables = 0;
	OPEN cur1;
	REPEAT
    FETCH cur1 INTO stablename,sbegintime,sendtime;
    	if not no_more_tables then
    		set v_insql = 'insert into tmp_aiot(collecttime,wpid,deviceid,indicator_code,data)';
    		set v_insql = concat(v_insql,' select collect_time,wpid,deviceid,indicator_code,',rangeSql);
    		set v_insql = concat(v_insql,' from ',stablename);
    		set v_insql = concat(v_insql,' where wpid =',v_stationid);
    		set v_insql = concat(v_insql,' isactive = 1');
    		set v_insql = concat(v_insql,' and collect_time between ''',v_begintime,''' and ''',v_endtime,'''');
    		set v_insql = concat(v_insql,' and indicator_code = ''',v_indicatorcode,'''');
    		set v_insql = concat(v_insql,' and deviceid =',v_deviceid);
    		set v_insql = concat(v_insql,alarmSql);
    		set @v_sql=v_insql;
			prepare stmt from @v_sql;
			EXECUTE stmt;
		 	deallocate prepare stmt; 
	    end if;
    UNTIL  no_more_tables = 1  END REPEAT;
    close cur1;
	
	##处理临时表中的数据,将数据整合保存到tmp_table中
	set selectSql = concat(selectSql,collectSql,' as collecttime');
	##判断统计类型
	if locate('avgdata',v_stattypes) then
		set selectSql = concat(selectSql,',ifnull(avg(data),0) as avgdata');
		set v_sql = concat(v_sql,',','avgdata');
	end if;
	if locate('mindata',v_stattypes) then
		set selectSql = concat(selectSql,',ifnull(min(data),0) as mindata');
		set v_sql = concat(v_sql,',','mindata');
	end if;
	if locate('maxdata',v_stattypes) then
		set selectSql = concat(selectSql,',ifnull(max(data),0) as maxdata');
		set v_sql = concat(v_sql,',','maxdata');
	end if;
	if locate('diffdata',v_stattypes) then
		set selectSql = concat(selectSql,',(ifnull(max(data),0) - ifnull(min(data),0)) as diffdata');
		set v_sql = concat(v_sql,',','diffdata');
	end if;
	if locate('amplidata',v_stattypes) then
		set selectSql = concat(selectSql,', case when ifnull(min(data),0) = 0 then 0 else (ifnull(max(data),0) - ifnull(min(data),0))/ifnull(min(data),0)*100 end as amplidata');
		set v_sql = concat(v_sql,',','amplidata');
	end if;
	set v_sql = concat(v_sql,') ',selectSql,' from tmp_aiot group by ',collectSql);
	set @v_sql=v_sql;
	prepare stmt from @v_sql;
	EXECUTE stmt;
	deallocate prepare stmt; 
	
	##更新最大值的时间
	if locate('maxdata',v_stattypes) then
			 	set v_sql = '';
		   		set v_sql = concat(v_sql,' update tmp_table a set maxtime = ( ');
		   		set v_sql = concat(v_sql,' select b.collecttime from tmp_aiot b');
		   		set v_sql = concat(v_sql,' where b.data = a.maxdata  and a.collecttime = ',collectB,' limit 1)');
		   		set @v_sql=v_sql;
				prepare stmt from @v_sql;
				EXECUTE stmt;
			 	deallocate prepare stmt; 
	end if;
	if locate('mindata',v_stattypes) then
			 	set v_sql = '';
		   		set v_sql = concat(v_sql,' update tmp_table a set mintime = ( ');
		   		set v_sql = concat(v_sql,' select b.collecttime from tmp_aiot b');
		   		set v_sql = concat(v_sql,' where b.data = a.mindata  and a.collecttime = ',collectB,' limit 1)');
		   		set @v_sql=v_sql;
				prepare stmt from @v_sql;
				EXECUTE stmt;
			 	deallocate prepare stmt; 
	end if;
	select * from tmp_table;
end;