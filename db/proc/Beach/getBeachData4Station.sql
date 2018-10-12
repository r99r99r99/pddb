## call getBeachData4Station(10004,'2018-06-08 14');
drop procedure if exists getBeachData4Station;
create procedure getBeachData4Station(
	in v_stationid	int,
	in v_collecttime varchar(20)
)
begin
	DECLARE no_more_tables INT DEFAULT 0;  
	DECLARE V_CODE VARCHAR(50);
	DECLARE V_TYPEID INT;
	DECLARE V_DATA DOUBLE;
	DECLARE S_DATA DOUBLE;
	declare v_sql varchar(5000) ; 
	
	DECLARE V_TABLENAME VARCHAR(30);
	
	DECLARE V_DEVICEID INT;
	DECLARE V_INDICATORCODE VARCHAR(30);
	DECLARE V_TOUNIT DOUBLE;
	
	##创建遍历临时表的游标
	DECLARE CUR1 CURSOR FOR
	SELECT CODE,TYPEID,DATA FROM TMP_TABLE;
	DECLARE CONTINUE HANDLER FOR NOT FOUND  SET  no_more_tables = 1;
	
	##创建存储结果的临时表
	drop table if exists tmp_table;
	CREATE TEMPORARY TABLE tmp_table
	(	 
		code varchar(50),
		typeid int,
		data double,
		title varchar(100)
	);
	
	##根据当前站点以及查询时间获得需要查询的表名
	SELECT TABLENAME INTO V_TABLENAME
	FROM AIOT_META_TABLE
	WHERE TYPE = 1 AND WPID = V_STATIONID
	AND ISACTIVE = 1 
	AND BEGINTIME <= V_COLLECTTIME AND ENDTIME >= V_COLLECTTIME
	LIMIT 1;
	##将海水浴场评价的code信息添加到临时表中
	insert into tmp_table(code,typeid)
	select code,type as typeid
	from aiot_beach_code;
	
	##根据当前站点以及时间获得配置表里的值
	UPDATE TMP_TABLE A,AIOT_BEACH_DATA B
	SET A.DATA = B.DATA
	WHERE A.CODE = B.CODE 
	AND B.STATIONID = V_STATIONID
	AND B.COLLECTTIME = V_COLLECTTIME;
	##根据数值,修改title
	update tmp_table a,aiot_beach_config b
	set a.title = b.value
	where b.code = a.code
	and a.typeid = 2
	and a.data = b.data;
	
	OPEN cur1;
	REPEAT
    FETCH cur1 INTO V_CODE,V_TYPEID,V_DATA;
		if not no_more_tables then
			if V_TYPEID = 1 and V_DATA is null then
				set v_deviceid = null;
				set v_indicatorcode = null;
				##从系统中读取数据
				SELECT deviceid,indicatorcode,tounit into v_deviceid,v_indicatorcode,v_tounit
				from aiot_beach_target
				where code = v_code
				and stationid = v_stationid;
				if v_deviceid is not null and v_indicatorcode is not null then
					IF V_TABLENAME IS NOT NULL THEN 
						set v_sql = '';
						set v_sql = concat(v_sql,' update tmp_table set data = ');
						set v_sql = concat(v_sql,' (SELECT DATA*',v_tounit);
						set v_sql = concat(v_sql,' FROM ',V_TABLENAME);
						set v_sql = concat(v_sql,' WHERE WPID =',V_STATIONID);
						set v_sql = concat(v_sql,' AND INDICATOR_CODE = ''',V_INDICATORCODE,'''');
						set v_sql = concat(v_sql,' AND COLLECT_TIME <=''',V_COLLECTTIME,'''');
						set v_sql = concat(v_sql,' AND COLLECT_TIME >= DATE_ADD(''',V_COLLECTTIME,''' ,INTERVAL -4 HOUR)');
						set v_sql = concat(v_sql,' ORDER BY COLLECT_TIME DESC LIMIT 1 )');
						set v_sql = concat(v_sql,' where code = ''',V_CODE,'''');
						set @v_sql=v_sql;
						prepare stmt from @v_sql;
						EXECUTE stmt;
		 				deallocate prepare stmt; 
					END IF;
				end if;
			end if;
		end if;
    UNTIL  no_more_tables = 1  END REPEAT;
    close cur1;
    update tmp_table set data=-1 where typeid = 1 and data is null ;
    update tmp_table set title = '无上报' where typeid = 1;
    update tmp_table set data=-1  where typeid = 2 and data is null;
    update tmp_table set title='无上报'  where typeid = 2 and title is null;
    
    SELECT * FROM TMP_TABLE;
end ;