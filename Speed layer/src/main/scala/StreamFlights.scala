import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.util.Bytes

object StreamFlights {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")
  
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val weatherDelaysByRoute = hbaseConnection.getTable(TableName.valueOf("taxi_revenue_byarea"))

  def incrementDelaysByRoute(kfr : KafkaTaxiRecord) : String = {
    println(kfr)
    val key=kfr.month+"-"+kfr.day+"-"+kfr.hour+"-"+kfr.minute+"-"+kfr.area
    val inc = new Increment(Bytes.toBytes(key))
    inc.addColumn(Bytes.toBytes("total"),Bytes.toBytes("total_count"),1);
    inc.addColumn(Bytes.toBytes("time"),Bytes.toBytes("end_hour"),kfr.ehour);
    inc.addColumn(Bytes.toBytes("time"),Bytes.toBytes("end_minute"),kfr.eminute);
    inc.addColumn(Bytes.toBytes("area"),Bytes.toBytes("trip_miles"),kfr.tripm);
    inc.addColumn(Bytes.toBytes("fee"),Bytes.toBytes("fare"),kfr.fare);
    inc.addColumn(Bytes.toBytes("fee"),Bytes.toBytes("tip"),kfr.tips);
    inc.addColumn(Bytes.toBytes("fee"),Bytes.toBytes("trip_total"),kfr.total);
    weatherDelaysByRoute.increment(inc)
    return "Updated speed layer for" +key
  }
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamFlights <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }
    
    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamTaxis")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("guahuang_taxi")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val serializedRecords = stream.map(_.value);

    val kfrs = serializedRecords.map(rec => mapper.readValue(rec, classOf[KafkaTaxiRecord]))

    // Update speed table    
    val processedFlights = kfrs.map(incrementDelaysByRoute)
    processedFlights.print()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
