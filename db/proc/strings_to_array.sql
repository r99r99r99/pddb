##call strings_to_array('cc#2,cqj#2,fdcj#2,laji#2,lang#2,oil#2,oilpu#2,pfw#2,qtemp#0,rjy#2,scw#2,see#2,sm#5,tmd#2,weather#2,wind#2,wtemp#33,zao#2',',')
drop PROCEDURE if exists strings_to_array;  
create PROCEDURE strings_to_array(s_str varchar(128), s_split varchar(3))  
begin  
	
  drop table if exists t_table;
  CREATE TEMPORARY TABLE t_table
	(	 
		param varchar(20)
	);
	
  set @i = length(s_str) - length(replace(s_str,s_split,''));  
  set @left_str = s_str;  
  while @i>0                
	  do   
	  set @sub_str = substr(@left_str,1,instr(@left_str,s_split)-1);              
	  set @left_str = substr(@left_str,length(@sub_str)+length(s_split)+1);     
	  set @n = trim(@sub_str);  
	  insert into t_table(param) values(@n); 
	  set @i = @i - 1;        
  end while;  

  select * from t_table;
  
end;  