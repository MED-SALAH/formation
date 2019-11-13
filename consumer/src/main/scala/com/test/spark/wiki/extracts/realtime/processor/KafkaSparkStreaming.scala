package com.test.spark.wiki.extracts.realtime.processor
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaSparkStreaming {


  def main(args: Array[String]): Unit = {

    // read the configuration file
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Transaction")
    val ssc = new StreamingContext(sparkConf, Seconds(5))



    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "http://15.188.51.222:9092",
      "key.deserializer" -> classOf[LongDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topicsSet = Array("Transaction")
    val stream: InputDStream[org.apache.kafka.clients.consumer.ConsumerRecord[Long, String]] =  KafkaUtils.createDirectStream(ssc,
      PreferConsistent,
      Subscribe[Long,String](topicsSet,kafkaParams))
    val transactionDSm: org.apache.spark.streaming.dstream.DStream[String] = stream.map(_.value )

    transactionDSm.print()

    ssc.start() // start the streaming context
    ssc.awaitTermination() // block while the context is running (until it's stopped by the timer)
    ssc.stop() // this additional stop seems to be required

  }
}
