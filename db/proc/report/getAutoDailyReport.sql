## call getAutoDailyReport(10011,'2018-09-20');
drop procedure if exists getAutoDailyReport;
create procedure getAutoDailyReport(
	in v_stationid	int,    ##站点编码
	in v_reportDate varchar(30)
)
begin
	DECLARE no_more_tables INT DEFAULT 0;  
	DECLARE i_stationname varchar(40);
	declare i_watertype int;   ##站点的水质类型
	DECLARE i_reportText text default '';
	DECLARE i_reportDate varchar(20);  ##格式化后的时间
	DECLARE i_tableName varchar(30);   ##查询该日期需要查询的表名	
	
	DECLARE v_insql text default '';    ##执行插入临时表的sql语句
	declare rangeSql text default '';
	declare i_mindata double;
	declare i_maxdata double;
	DECLARE i_deviceid int;
	DECLARE i_indicatorcode varchar(30);
	
	declare alarmSql text default '';
	declare i_alarmData double;  
	declare i_beginTime varchar(20);
	declare i_endTime varchar(20);
	
	##定义水质等级部分
	declare i_levelid int;
	declare i_levelname varchar(20);
	declare i_indicatorname varchar(30);
	declare i_allindicator varchar(2000);
	declare i_alllevelid int default 0;
	declare i_alllevelname varchar(20) default '';
	
	##定义各参数统计部分
	declare s_indicatorname varchar(30);
	declare s_avgdata double;
	declare s_mindata double;
	declare s_maxdata double;
	declare s_unitname varchar(20) default '';
	
	##遍历参数统计结果
	declare cur0 cursor for
	select indicatorname,avgdata,maxdata,mindata,unitname
	from tmp_indicator;
	
	##遍历tmp_level表,
	DECLARE CUR1 CURSOR FOR
	SELECT indicatorname,levelid,levelname
	from tmp_level;
	
	##查询站点 设备的报警码信息
	DECLARE CUR2 CURSOR FOR 
	SELECT alarmdata,begintime,endtime,deviceid
	from aiot_device_alarm
	where stationid = v_stationid;
	
	##查询站点 设备参数的监测量程
	DECLARE CUR3 CURSOR FOR 
	SELECT mindata,maxdata,deviceid,indicatorcode
	from aiot_range_data
	where stationid = v_stationid;
	
	DECLARE CONTINUE HANDLER FOR NOT FOUND  SET  no_more_tables = 1;
	##创建最终查询的临时表
	drop table if exists tmp_result; 
	CREATE TEMPORARY TABLE tmp_result
	(	 
		reportTitle		varchar(400),
		reportText 		text
	);
	
	drop table if exists tmp_statis;
	CREATE TEMPORARY TABLE tmp_statis
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
	
	
	##定义最终设备查询的参数列表
	drop table if exists tmp_indicator;
	CREATE TEMPORARY TABLE tmp_indicator
	(	 
		indicatorcode		varchar(30) not null primary key,
		indicatorname		varchar(40),
		unitname			varchar(20),
		avgdata			double ,
		maxdata			double ,
		maxtime			varchar(20),
		mindata			double ,
		mintime 		varchar(20)
	);
	
	drop table if exists tmp_level;
	CREATE TEMPORARY TABLE tmp_level
	(	 
		indicatorcode		varchar(30),
		indicatorname		varchar(30),
		levelid			int,
		levelname 		varchar(20)
	);
	
	
	##获得当前站点的名称以及水质类型
	select title,watertype into i_stationname,i_watertype
	from aiot_watch_point 
	where id = v_stationid;
	##开始编写标题
	insert into tmp_result(reportTitle)
	values(concat(i_stationname,v_reportDate,'日报'));
	##开始编写日报内容
	select date_format(v_reportDate,'%Y年%m月%e日') into i_reportDate;
	
	set i_reportText = concat(i_reportText,i_reportDate);
	
	##获得查询该条记录需要查询的表名
	select a.tablename into i_tablename
	from aiot_meta_table a
	where wpid = v_stationid
	and date_format(begintime,'%Y-%m-%d') <= date_format(v_reportdate,'%Y-%m-%d')
	and date_format(endtime,'%Y-%m-%d') > date_format(v_reportdate,'%Y-%m-%d')
	and type = 1 and isactive = 1;
	
	##根据参数的检测量程,获得查询条件
	set rangeSql = concat(rangeSql,' case when 1=0 then 0 ');
    set no_more_tables = 0;
    OPEN cur3;
	REPEAT
    FETCH cur3 INTO i_mindata,i_maxdata,i_deviceid,i_indicatorcode;
    	if not no_more_tables then
    		set rangeSql = concat(rangeSql,' when a.data<',i_mindata,' and a.deviceid = ',i_deviceid,' and a.indicator_code = ''',i_indicatorcode,''' then ',i_mindata);
    		set rangeSql = concat(rangeSql,' when a.data>',i_maxdata,' and a.deviceid = ',i_deviceid,' and a.indicator_code = ''',i_indicatorcode,''' then ',i_maxdata);
	    end if;
    UNTIL  no_more_tables = 1  END REPEAT;
    close cur3;
    set rangeSql = concat(rangeSql,' else data end as data');
    
    ##查询该站点 设备的设备报警码
    set no_more_tables = 0;
	OPEN cur2;
	REPEAT
    FETCH cur2 INTO i_alarmData,i_beginTime,i_endTime,i_deviceid;
    	if not no_more_tables then
    		set alarmSql = concat(alarmSql,' and !(');
    		set alarmSql = concat(alarmSql,' data = ',i_alarmData);
    		set alarmSql = concat(alarmSql,' and collect_time >=''',i_beginTime,'''');
    		set alarmSql = concat(alarmSql,' and collect_time <=''',i_endTime,'''');
    		set alarmSql = concat(alarmSql,' and a.deviceid = ',i_deviceid);
    		set alarmSql = concat(alarmSql,' )');
	    end if;
    UNTIL  no_more_tables = 1  END REPEAT;
    close cur2;
    
    ##该站点需要查询的参数
    insert into tmp_indicator(indicatorcode,indicatorname,unitname)
	select distinct indicatorcode,b.title as indicatorname,c.logo as unitname
	from view_stationid_deviceid_indicatorcode a,dm_indicator b,g_unit c
	where stationid = v_stationid
	and a.indicatorcode = b.code
	and b.unitid = c.id
	;
	
	##将该站点当天的所有记录插入到临时表中
	set v_insql = concat(v_insql,' insert into tmp_aiot(collecttime,wpid,deviceid,indicator_code,data)');
	set v_insql = concat(v_insql,' select a.collect_time,a.wpid,a.deviceid,a.indicator_code,',rangeSql);
    set v_insql = concat(v_insql,' from ',i_tablename,' a,view_stationid_deviceid_indicatorcode b');
    set v_insql = concat(v_insql,' where wpid =',v_stationid);
    set v_insql = concat(v_insql,' and b.stationid =',v_stationid);
    set v_insql = concat(v_insql,' and a.deviceid = b.deviceid');
    set v_insql = concat(v_insql,' and a.indicator_code = b.indicatorcode');
    set v_insql = concat(v_insql,' and a.isactive  = 1');
    set v_insql = concat(v_insql,' and date_format(collect_time,''%Y-%m-%d'') = date_format(''',v_reportdate,''',''%Y-%m-%d'')');
    set v_insql = concat(v_insql,alarmSql);
	
	set @v_sql=v_insql;
	prepare stmt from @v_sql;
	EXECUTE stmt;
	deallocate prepare stmt; 
	
	##统计该站点的参数平均值\最大值\最小值
	insert into tmp_indicator(indicatorcode,avgdata,maxdata,mindata)
	select indicator_code,avg(data) as avgdata,max(data) as maxdata,min(data) as mindata
	from tmp_aiot
	group by indicator_code
	on duplicate key update avgdata = values(avgdata),maxdata=values(maxdata),mindata=values(mindata);
	##删除平均值为空的数据
	delete from tmp_indicator where avgdata is null;	
	###############查询该站点当天的水质等级
	##根据以上统计的平均值来计算水质等级
	insert into tmp_level(indicatorcode,indicatorname,levelid,levelname)
	select a.indicatorcode,a.indicatorname,c.classid,c.classname
	from tmp_indicator a,waterqualitystandard b,g_waterstandard_config c
	where a.indicatorcode = b.item
	and c.typeid = i_watertype
	and b.standard_grade = c.classid
	and b.water_type = i_watertype
	and case b.min when 1 then a.avgdata >= min_value else a.avgdata>min_value end
	and case b.max when 1 then a.avgdata <= max_value else a.avgdata<max_value end
	;
	
	
	set no_more_tables = 0;
	OPEN cur1;
	REPEAT
    FETCH cur1 INTO i_indicatorname,i_levelid,i_levelname;
    	if not no_more_tables then
    		if i_levelid > i_alllevelid then
    			set i_allindicator = i_indicatorname;
    			set i_alllevelid = i_levelid;
    			set i_alllevelname = i_levelname;
    		elseif i_levelid = i_alllevelid then
    		    set i_allindicator = concat(i_allindicator,',',i_indicatorname);
    		end if;
    		
	    end if;
    UNTIL  no_more_tables = 1  END REPEAT;
    close cur1;
    
	set i_reportText = concat(i_reportText,',',i_stationname);
	set i_reportText = concat(i_reportText,'的水质等级为',i_alllevelname,'水质,其中,首要因子为');
	set i_reportText = concat(i_reportText,i_allindicator,'.');
	set i_reportText = concat(i_reportText,'当天检测结果如下:');
	
	set no_more_tables = 0;
	OPEN cur0;
	REPEAT
    FETCH cur0 INTO s_indicatorname,s_avgdata,s_maxdata,s_mindata,s_unitname;
    	if not no_more_tables then
    		if s_unitname is null then 
    			set s_unitname = '';
    		end if;
    		set i_reportText = concat(i_reportText,s_indicatorname,'检测的平均值为',s_avgdata,s_unitname);
    		set i_reportText = concat(i_reportText,',最大值为',s_maxdata,s_unitname);
    		set i_reportText = concat(i_reportText,',最小值为',s_mindata,s_unitname);
    		set i_reportText = concat(i_reportText,';');
	    end if;
    UNTIL  no_more_tables = 1  END REPEAT;
    close cur0;
	
	
	
	set i_reportText = concat(i_reportText,'');
	##将日报内容保存到结果表中
	update tmp_result set reportText = i_reportText;
	select * from tmp_result;
end;