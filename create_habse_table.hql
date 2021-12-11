'''
Get into hive
'''

beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver


'''
Create table
'''

drop table taxi_revenue_byarea;
create table taxi_revenue_byarea(
  pickuparea_date string,
  total_order_count bigint,
  end_hour_sum bigint,
  end_minute_sum bigint,
  trip_miles_sum bigint,
  fare_sum bigint,
tips_sum bigint,
trip_total bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,total:total_count#b,time:end_hour#b,time:end_minute#b,area:trip_miles#b,fee:fare#b,fee:tip#b,fee:trip_total#b')
TBLPROPERTIES ('hbase.table.name' = 'taxi_revenue_byarea');

insert overwrite table taxi_revenue_byarea
select concat(start_month,"-",start_day,"-",start_hour,"-",start_minute,"-",pickup_community_area) as pickuparea_date,
  count(1) as total_order_count,
  sum(end_hour) as end_hour_sum,
  sum(end_minute) as end_minute_sum,
  sum(trip_miles) as trip_miles_sum ,
  sum(fare) as fare_sum,
 sum(tips) as tips_sum,
sum(trip_total) as trip_total_sum from guahuang_taxi 
where pickup_community_area is not null and dropoff_community_area is not null and trip_total is not null
group by pickup_community_area,start_month,start_day,start_hour,start_minute 
order by trip_total_sum;

'''
Select sentences
'''
select * from taxi_revenue_byarea  limit 5; 
