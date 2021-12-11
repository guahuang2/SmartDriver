import os
import glob
import pandas as pd
from datetime import datetime
dateparse = lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S')
os.chdir("taxi_trip_data")

extension = 'csv'
all_filenames = ["chicago_taxi_trips_2016_0"+str(i)+".csv" for i in range(1,4)]
print(len(all_filenames))
# #combine all files in the list
csv_list=[]
for f in all_filenames:
    # csvFile=pd.read_csv(f,parse_dates=[1,2],date_parser=dateparse)
    csvFile=pd.read_csv(f)
    csvFile['trip_start_timestamp']= pd.to_datetime(csvFile['trip_start_timestamp'],format='%Y-%m-%d %H:%M:%S')
    csvFile['trip_end_timestamp']= pd.to_datetime(csvFile['trip_end_timestamp'],format='%Y-%m-%d %H:%M:%S')
    csvFile['start_year']= csvFile['trip_start_timestamp'].dt.year
    csvFile['start_month']= csvFile['trip_start_timestamp'].dt.month
    csvFile['start_day']= csvFile['trip_start_timestamp'].dt.day
    csvFile['start_hour']= csvFile['trip_start_timestamp'].dt.hour
    csvFile['start_minute']= csvFile['trip_start_timestamp'].dt.minute
    csvFile['end_year']= csvFile['trip_end_timestamp'].dt.year
    csvFile['end_month']= csvFile['trip_end_timestamp'].dt.month
    csvFile['end_day']= csvFile['trip_end_timestamp'].dt.day
    csvFile['end_hour']= csvFile['trip_end_timestamp'].dt.hour
    csvFile['end_minute']= csvFile['trip_end_timestamp'].dt.minute
    csvFile.drop(columns=['trip_start_timestamp', 'trip_end_timestamp','pickup_census_tract','dropoff_census_tract'])
    csvFile.dropna()
    csv_list.append(csvFile)
combined_csv = pd.concat(csv_list)
#export to csv
combined_csv.to_csv( "combined.csv", header=False,index=False, encoding='utf-8-sig')

'''
taxi_id,trip_start_timestamp,trip_end_timestamp
trip_seconds,trip_miles,pickup_census_tract,dropoff_census_tract
pickup_community_area,dropoff_community_area,fare,tips,tolls,extras,trip_total,payment_type,company,pickup_latitude,pickup_longitude,dropoff_latitude,dropoff_longitude
'''