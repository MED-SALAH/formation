package kafka

import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SQLContext, SparkSession}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, Seconds, StateSpec, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.dstream.DStream
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.datastax.driver.core.Cluster
import com.datastax.spark.connector._


object Consumer {

  def main(args: Array[String]): Unit = {
    implicit val encd = Encoders.product[Dstreams]

    val conf = new SparkConf().setMaster("local[*]").setAppName("test2")
    conf.set("spark.cassandra.connection.host","192.168.1.57"); //cassandra host
    //conf.set("spark.streaming.backpressure.enabled", "true")
    implicit val spark: SparkSession=SparkSession
      .builder()
      .master("local[*]")
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext // create a new spark core context
    val ssc = new StreamingContext(sc, Seconds(1))

    // read the configuration file

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "35.180.127.210:9092",
      "key.deserializer" -> classOf[LongDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topicsSet = Array("Transaction")

    val stream: InputDStream[ConsumerRecord[Long, String]] =  KafkaUtils.createDirectStream(ssc,
      PreferConsistent,
      Subscribe[Long,String](topicsSet,kafkaParams))

    stream.start()


    val dstream = stream
      .foreachRDD(record => {
        val roW = record.map(re => {
          (re.topic(), re.key(), re.value(), re.partition(), re.offset())
        }).take(10)
        println(roW.foreach(r=>println(r)))
//        reDF.show()
      })


//    str.foreachRDD(record => print("qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"+record))

    println("Starting....Listen to ")

    ssc.start()
    ssc.awaitTermination()// Start the computation
     // Wait for the computation to terminate

  }

}
