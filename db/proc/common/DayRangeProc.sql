## Call DayRangeProc('2010-9-1','2010-9-10',1);
DROP PROCEDURE IF EXISTS DayRangeProc;
CREATE PROCEDURE DayRangeProc(
	IN start_date datetime , 
	IN end_date datetime ,
	IN typeid int  ##  1代表按日  2代表按周 3代表按月
)
BEGIN
    DECLARE temp  INT;
    DECLARE range_day  INT;
    DECLARE s_sql varchar(100);
    DECLARE s_group varchar(50);
    ##创建存储时间的临时表\
    drop table if exists tmp_date;
	CREATE TEMPORARY TABLE tmp_date
	(	 
		xtime			datetime
		
	);

    SET temp = 0;
    SET range_day = (SELECT DATEDIFF(end_date,start_date));

    WHILE temp  <= range_day DO
    	  insert into tmp_date(xtime)
          select ADDDATE(start_date, temp);
          SET temp = temp + 1;
    END WHILE;
    if typeid=3 then
    	set s_group = 'date_format(xtime,''%Y-%m'')';
    elseif typeid = 2 then
    	set s_group = 'concat(year(xtime),week(xtime))';
    else 
    	set s_group = 'xtime';
    end if;
    set s_sql = concat(' select ',s_group,' as xtime');
    set s_sql = concat(s_sql,' from tmp_date');
    set @v_sql=s_sql;
	prepare stmt from @v_sql;
	EXECUTE stmt;
	deallocate prepare stmt; 
END;