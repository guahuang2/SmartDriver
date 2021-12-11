Drop table guahuang_taxi;
CREATE EXTERNAL TABLE IF NOT EXISTS guahuang_taxi (
    taxi_id int,
    trip_start_timestamp string ,
    trip_end_timestamp string ,
    trip_seconds int,
    trip_miles float ,
    pickup_census_tract int ,
    dropoff_census_tract int,
    pickup_community_area int,
    dropoff_community_area int,
    fare float,
    tips float, 
    tolls float,
    extras float,
    trip_total float,
    payment_type string,
    company int ,
    pickup_latitude int ,
    pickup_longitude int ,
    dropoff_latitude int ,
    dropoff_longitude int,
     start_year int,
 start_month int,
start_day int,
 start_hour int,
  start_minute int,
  end_year int,
   end_month int,
  end_day int,
   end_hour int,
    end_minute int)
 COMMENT 'taxi Table'
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ',';

LOAD DATA local INPATH '/home/hadoop/guahuang/project' INTO TABLE guahuang_taxi;