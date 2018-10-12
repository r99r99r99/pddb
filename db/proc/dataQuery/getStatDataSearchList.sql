## call getStatDataSearchList(10004,3,'2018-07-09','2018-08-08');
drop procedure if exists getStatDataSearchList;
create procedure getStatDataSearchList(
	in v_stationid	int,    ##站点编码
	in v_statType int,      ##统计口径  1按月统计  2按周统计  3按日统计
	in v_beginDate varchar(20),   ##开始时间
	in v_endDate varchar(20) 		##结束时间
)
begin
	DECLARE no_more_tables INT DEFAULT 0;  
	DECLARE i_groupSql varchar(50) ;
	declare i_watertype int;  ##当前站点的水质类型
	
	declare i_indicatorid int;
	declare i_indicatorcode varchar(30);
	declare i_indicatorname varchar(30);
	
	declare i_deviceid int;
	declare i_alarmdata double;
	declare i_begintime varchar(20);
	declare i_endtime varchar(20);
	
	declare i_tablename varchar(50);
	
	##定义SQL语句部分
	DECLARE indicatorSql varchar(20000) default '';  ##定义每个参数的插入临时表的sql语句
	DECLARE alarmSql varchar(10000) default '';   ##定义设备报警码语句
	DECLARE insql varchar(10000) default '';  
	DECLARE isfirst boolean default true;
	
	##获得水质标准的配置表
	DECLARE CUR1 CURSOR FOR 
	select distinct b.id as indicatorid,b.code,b.title
	from waterqualitystandard a,dm_indicator b
	where a.water_type = i_watertype
	and a.item = b.code and b.isactive = 1
	order by a.id desc;
	
	##查询该站点所有设备的设备报警码
	DECLARE CUR2 CURSOR FOR
	SELECT deviceid,alarmdata,begintime,endtime
	from aiot_device_alarm
	where stationid = v_stationid
	and begintime <= v_endDate
	and endtime >= v_beginDate;
	
	##查询需要查询的表
	DECLARE CUR3 CURSOR FOR
	SELECT TABLENAME
	FROM aiot_meta_table  
	WHERE wpid = v_stationid AND TYPE = 1 
	AND BEGINTIME <= v_endDate and endtime > v_begindate;
	
	DECLARE CONTINUE HANDLER FOR NOT FOUND  SET  no_more_tables = 1;
	
	drop table if exists tmp_middle;   ##临时表获得每个统计口径下的水质等级以及参数
	CREATE TEMPORARY TABLE tmp_middle
	(	 
		xtime   varchar(20),
		name 	varchar(300),
		ydata   int,
		UNIQUE KEY `ssss` (`xtime`) USING BTREE
	);
	
	
	##查询该站点所有设备的设备报警码
	set no_more_tables = 0;	 	
	OPEN CUR2;
	REPEAT
	FETCH CUR2 INTO  i_deviceid,i_alarmdata,i_begintime,i_endtime;
		if not no_more_tables then
		 	set alarmSql = concat(alarmSql,' and !(');
		 	set alarmSql = concat(alarmSql,' wpid =',v_stationid);
		 	set alarmSql = concat(alarmSql,' and deviceid =',i_deviceid);
		 	set alarmSql = concat(alarmSql,' and collect_time >= ''',i_begintime,'''');
		 	set alarmSql = concat(alarmSql,' and collect_time <= ''',i_endtime,'''');
		 	set alarmSql = concat(alarmSql,' and data = ',i_alarmdata);
		 	set alarmSql = concat(alarmSql,' )');
		end if;
	UNTIL  no_more_tables = 1  END REPEAT;
	close CUR2;	
	
	
	
	
	##根据参数判断统计口径
	if v_stattype = 1 then 
		set i_groupSql = 'date_format(collect_time,''%Y-%m'')';
	elseif v_stattype = 2 then 
		set i_groupSql = 'concat(year(collect_time),week(collect_time))';
	elseif v_stattype = 3 then
		set i_groupSql = 'date_format(collect_time,''%Y-%m-%d'')';
	end if;
	
	##获得当前站点的水质类型
	select watertype into i_watertype
	from aiot_watch_point 
	where id = v_stationid;
	
	set no_more_tables = 0;	 	
	OPEN CUR1;
	REPEAT
	FETCH CUR1 INTO  i_indicatorid,i_indicatorcode,i_indicatorname;
		if not no_more_tables then
			set isfirst =true;
			set insql = '';
			set indicatorsql = ' insert into tmp_middle(xtime,name,ydata)';
			set indicatorsql = concat(indicatorsql,' select xtime,name,ydata from (');
			set indicatorsql = concat(indicatorsql,' select a.collect_time as xtime,''',i_indicatorname ,''' as name,b.standard_grade as ydata ');
			set indicatorsql = concat(indicatorsql,' from (');
		 	##得到需要查询的表
		 	OPEN CUR3;
			REPEAT
			FETCH CUR3 INTO  i_tablename;
				if not no_more_tables then
				 	##得到需要查询的表
				 	if !isfirst then 
				 		set insql = concat(insql,' union all ');
				 	end if;
				 	set insql = concat(insql,' select ',i_groupSql,' as collect_time,avg(data) as data');
				 	set insql = concat(insql,' from ',i_tablename);
				 	set insql = concat(insql,' where date_format(collect_time,''%Y-%m-%d'') between date_format(''',v_begindate,''',''%Y-%m-%d'') and date_format(''',v_endDate,''',''%Y-%m-%d'')');
				 	set insql = concat(insql,' and wpid = ',v_stationid);
				 	set insql = concat(insql,' and indicator_code =''',i_indicatorcode,'''');
				 	set insql = concat(insql,' and collect_type = 1');
				 	set insql = concat(insql,' and isactive = 1 ');
				 	set insql = concat(insql,alarmSql);
				 	set insql = concat(insql,' group by ',i_groupSql);
				 	set isfirst =false;
				end if;
			UNTIL  no_more_tables = 1  END REPEAT;
			close CUR3;	
			set no_more_tables = 0;	 
			set indicatorsql = concat(indicatorsql,insql);
			set indicatorsql = concat(indicatorsql,' ) a,waterqualitystandard b ');
			set indicatorsql = concat(indicatorsql,' where b.item = ''',i_indicatorcode,''' and b.water_type = ',i_watertype);
			set indicatorsql = concat(indicatorsql,' and case when b.min=1 then a.data >= b.min_value else a.data > b.min_value end ');
			set indicatorsql = concat(indicatorsql,' and case when b.max=1 then a.data <= b.max_value else a.data < b.max_value end');
			set indicatorsql = concat(indicatorsql,' order by a.collect_time ) mm');
			set indicatorsql = concat(indicatorsql,' on duplicate key  ');
			set indicatorsql = concat(indicatorsql,' update ');
			set indicatorsql = concat(indicatorsql,' name = case when mm.ydata>tmp_middle.ydata then mm.name ');
			set indicatorsql = concat(indicatorsql,' when mm.ydata=tmp_middle.ydata then concat(tmp_middle.name,'','',mm.name) else tmp_middle.name end ,');
			set indicatorsql = concat(indicatorsql,' ydata = case when mm.ydata > tmp_middle.ydata then mm.ydata else tmp_middle.ydata  end');
			set @v_sql=indicatorsql;
			prepare stmt from @v_sql;
			EXECUTE stmt;
			deallocate prepare stmt; 	
		end if;
	UNTIL  no_more_tables = 1  END REPEAT;
	close CUR1;	
	select * from tmp_middle order by xtime;
end;