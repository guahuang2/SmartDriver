#Maxprofit taxi driver
## Introduction
The Smart Driver application is a big data application that implements lambda architecture. It aims at helping taxi drivers to decide where they should go to pick up customers by providing possible order information in the taxi’s surrounding blocks. 

### Screen shot
![serving_layer](/screenshot/serving_layer.PNG?raw=true "serving_layer")
![speed_layer](/screenshot/speed_layer.PNG?raw=true "speed_layer")

### Demo video
Please check demo_video.mp4

## Dataset 
[Chicago Taxi Rides 2016](https://www.kaggle.com/chicago/chicago-taxi-rides-2016?select=data_dictionary.csv).

## Data preprocessing 
Run combine_csv.py to combine mutliple taxi csv records into one 

## Create hive and hbase table
See the creaet_hive_talbe.hql and create_hbase_table.hql in the root directory

## Run the serving layer 
### Copy the application to the server
scp -i ~/.ssh/guahuang.pem -r ~/Desktop/app/ guahuang@ec2-52-14-115-151.us-east-2.compute.amazonaws.com:~/project
npm i kafka-node
npm install
### Run it 
node app.js 3711 172.31.39.49 8070 b-2.mpcs53014-kafka.198nfg.c7.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014-kafka.198nfg.c7.kafka.us-east-2.amazonaws.com:9092

## Run the speed layer
spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class StreamFlights uber-untitled3-1.0-SNAPSHOT.jar kafka_broker


 
