## call getBeachDetailByStation(10004);
drop procedure if exists getBeachDetailByStation;
create procedure getBeachDetailByStation(
	in v_stationId int
)
leave_name:
begin
	DECLARE no_more_tables INT DEFAULT 0;  
	DECLARE v_lastTime varchar(30);  ##最后一条数据的时间
	DECLARE V_CODE VARCHAR(20);
	DECLARE V_CLASSID INT DEFAULT 0;  ##环境等级	
	
	DECLARE I_CODE VARCHAR(30);
	DECLARE I_NAME VARCHAR(30);
	DECLARE I_qualityId INT;
	DECLARE I_qualityName VARCHAR(30);
	DECLARE I_qualityRemark VARCHAR(300);
	DECLARE I_qualityColor VARCHAR(30);
	
	DECLARE CUR1 CURSOR FOR
	SELECT DISTINCT CODE
	FROM AIOT_BEACH_DATA ;
	DECLARE CONTINUE HANDLER FOR NOT FOUND  SET  no_more_tables = 1;
	
	##创建保存数据的临时表
	drop table if exists tmp_table;
	CREATE TEMPORARY TABLE tmp_table
	(	 
		stationId int,
		stationName varchar(30),
	    collectTime varchar(20),
	    qualityId int,
	    qualityName varchar(20),
	    qualityRemark varchar(200),
	    qualityColor varchar(20),
	    indicatorName varchar(300),
		val varchar(20)
	);
	
	##获得该站点的最后一条数据的时间
	select max(collecttime) into v_lastTime
	from aiot_beach_data
	where stationid = v_stationId;
	##当没有数据时,跳出存储过程
	if v_lastTime is null or length(v_lastTime)<1 then
		select '1';
		LEAVE leave_name;
	end if;
	
	INSERT INTO tmp_table(STATIONID,COLLECTTIME) VALUES(V_STATIONID,v_lastTime);
	OPEN cur1;
	REPEAT
    FETCH cur1 INTO V_CODE;
    	if not no_more_tables then
    		SELECT A.CODE,C.NAME,B.LEVELID AS qualityId,D.NAME AS qualityName,D.REMARK AS qualityRemark,
  			D.COLOR AS qualityColor
  			 INTO I_CODE,I_NAME,I_qualityId,I_qualityName,I_qualityRemark,I_qualityColor
  			FROM AIOT_BEACH_DATA A,AIOT_BEACH_THRESHOLD B,AIOT_BEACH_CODE C,SYS_DOMAIN_LEVEL D
    		WHERE A.COLLECTTIME = V_LASTTIME
    		AND A.STATIONID = V_STATIONID
    		AND A.CODE = B.CODE AND A.CODE = C.CODE
    		AND A.CODE = V_CODE
    		AND (CASE WHEN C.TYPE = 1 THEN 
    				CASE WHEN B.MINCAL = 1 THEN A.DATA>=B.MIN ELSE A.DATA>B.MIN END
    				AND
    				CASE WHEN B.MAXCAL = 1 THEN A.DATA<=B.MAX ELSE A.DATA<B.MIN END
    			 ELSE 
           			 A.DATA = B.DATA
           		END )
			AND D.DOMAINID = 11
			AND B.LEVELID = D.CODE;
			
			##
			IF I_qualityId > V_CLASSID THEN
				SET V_CLASSID = I_qualityId;
				UPDATE tmp_table
				SET qualityId = I_qualityId,
				qualityName  = I_qualityName,
				qualityRemark = I_qualityRemark,
				qualityColor = I_qualityColor,
				indicatorName = I_NAME;
		 	ELSEIF I_qualityId = V_CLASSID THEN
		 		UPDATE tmp_table
		 		SET indicatorName = CONCAT(indicatorName,',',I_NAME);
			END IF;
			
		end if;
    UNTIL  no_more_tables = 1  END REPEAT;
    close cur1;

	
	
	select * FROM tmp_table;
	
end ;