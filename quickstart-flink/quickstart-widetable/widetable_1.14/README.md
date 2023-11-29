quick start widetable The green part of the picture is finished
<img width="859" alt="image" src="https://github.com/apache/incubator-streampark-quickstart/assets/91652711/7c556307-1cd7-4a4d-a9c1-66ccc948c59d">

The way to use it is very simple just pass the following parameters to use it:

     --sql-conf business-sql="SELECT cdc.biz_keeper_grade.id,cdc.biz_keeper_grade.bus_opp_num,cdc.biz_keeper_grade.keeper_grade_code,cdc.biz_keeper_grade.keeper_grade,cdc.biz_bus_opp.city_code,cdc.biz_bus_opp.remark,cdc.biz_house.room_num,cdc.biz_house.address FROM cdc.biz_keeper_grade LEFT JOIN cdc.biz_bus_opp ON cdc.biz_keeper_grade.bus_opp_num=cdc.biz_bus_opp.bus_opp_num LEFT JOIN cdc.biz_house ON cdc.biz_bus_opp.house_id=cdc.biz_house.id"

     --source-conf s1-username=mysqlcdc
     --source-conf s1-password=******
     --source-conf s1-hostname=10.30.238.23
     --source-conf s1-port=3306
     --source-conf s1-database=cdc
     --source-conf s1-tables=cdc.biz_keeper_grade,cdc.biz_bus_opp

     --source-conf s2-username=mysqlcdc
     --source-conf s2-password=******
     --source-conf s2-hostname=10.30.238.24
     --source-conf s2-port=3306
     --source-conf s2-database=cdc
     --source-conf s2-tables=cdc.biz_house

     --source-conf s[n]-...... More data sources can be configured*

     --source-conf scan.startup.mode=initial
     --source-conf scan.snapshot.fetch.size=10000

     --sink-conf connector=jdbc
     --sink-conf url=jdbc:mysql://10.30.238.25:3306/widetabledb?useSSL=false&useUnicode=true&characterEncoding=UTF-8
     --sink-conf table-name=big_table
     --sink-conf username=root
     --sink-conf password=******

     --dim-conf db-type=mysql
     --dim-conf url-param=useSSL=false&useUnicode=true&characterEncoding=UTF-8
     --dim-conf username=root
     --dim-conf password=******
     --dim-conf hostname=10.30.238.26
     --dim-conf port=3306
     --dim-conf database=demension

     --job-conf parallel=6
     --job-conf window-size=70
     --job-conf checkpoint-interval=10000

