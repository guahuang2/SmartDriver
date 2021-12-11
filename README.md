#Maxprofit taxi driver
## Introduction
The Smart Driver application is a big data application that implements lambda architecture. It aims at helping taxi drivers to decide where they should go to pick up customers by providing possible order information in the taxi’s surrounding blocks. 

### Screen shot
![serving_layer](/screenshot/serving_layer.PNG?raw=true "serving_layer")
![speed_layer](/screenshot/speed_layer.png?raw=true "speed_layer")

### Demo video
Please check demo_video.mp4

## Dataset 
[Chicago Taxi Rides 2016](https://www.kaggle.com/chicago/chicago-taxi-rides-2016?select=data_dictionary.csv).

## Data preprocessing 
Run combine_csv.py to combine mutliple taxi csv records into one 

## Create hive and hbase table
See the creaet_hive_talbe.hql and create_hbase_table.hql in the root directory

## Deploy the serving layer 
### Copy the serving layer app to the server and run
npm i kafka-node
npm install
### launch cmd 
node app.js 3711 172.31.39.49 8070 kafaka_broker

## Deploy the speed layer
spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class StreamFlights uber-untitled3-1.0-SNAPSHOT.jar kafka_broker
