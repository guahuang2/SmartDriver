#Maxprofit taxi driver
## Introduction
This big data application is mainly used for helping taxi drivers to decide where they should go to pick up customers. The dataset contains the record of taxi order in 2016. The serving layer provides the end time and payment information grouped by the pick up area. In this way, the taxi triver could look up several areas next to his current location to make decision that maximize his profit. The speed layer use manual input to simulate how the application will updated whenever an order is completed. It contains information such as start end time, trip distance and payments. And let's try submitting it. We could see that the key is updated in the backend. And let's do the query one more time in the look up page. We could see that the order total is updated and the average pay decreases a little bit. Thanks for wathcing.
### Screen shot
![serving_layer](/screenshot/serving_layer.PNG?raw=true "serving_layer")
![speed_layer](/screenshot/speed_layer.PNG?raw=true "speed_layer")

### Demo video
Please check demo_video.mp4

## Data preprocessing 
Run combine_csv.py to combine mutliple taxi csv records into one 

## Create hive and hbase table
See the creaet_hive_talbe.hql and create_hbase_table.hql under the root directory

## Run the serving layer 
### Copy the application to the server
scp -i ~/.ssh/guahuang.pem -r ~/Desktop/app/ guahuang@ec2-52-14-115-151.us-east-2.compute.amazonaws.com:~/project
npm i kafka-node
npm install
### Run it 
node app.js 3711 172.31.39.49 8070 b-2.mpcs53014-kafka.198nfg.c7.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014-kafka.198nfg.c7.kafka.us-east-2.amazonaws.com:9092

## Run the speed layer
spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class StreamFlights uber-untitled3-1.0-SNAPSHOT.jar b-2.mpcs53014-kafka.198nfg.c7.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014-kafka.198nfg.c7.kafka.us-east-2.amazonaws.com:9092


 
