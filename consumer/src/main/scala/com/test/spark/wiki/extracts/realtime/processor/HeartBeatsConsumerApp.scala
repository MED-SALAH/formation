package com.test.spark.wiki.extracts.realtime.processor

import java.util.Date

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.test.spark.wiki.extracts.domains.{FormationConfig, HeartBeatCs, TX}
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Milliseconds, State, StateSpec, StreamingContext}
object HeartBeatsConsumerApp {

  val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)

  def parse(json:String):HeartBeatCs = {
    objectMapper.readValue(json, classOf[HeartBeatCs])
  }

  def main(args: Array[String]): Unit = {

    implicit val  encoder = Encoders.product[HeartBeatCs]
    // read the configuration file
    val sparkConf = new SparkConf().setAppName("HeartBeat")
    sparkConf.setIfMissing("spark.master", "local[*]")
    sparkConf.set("spark.streaming.backpressure.enabled", "true")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Milliseconds(500))
    ssc.checkpoint("./chekpoint")

    println(s"Starting....Listen to ${FormationConfig.BOOTSTRAP_SERVERS}")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> FormationConfig.BOOTSTRAP_SERVERS,
      "key.deserializer" -> classOf[LongDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topicsSet = Array("HeartBeat")

    val stream: InputDStream[org.apache.kafka.clients.consumer.ConsumerRecord[Long, String]] =  KafkaUtils.createDirectStream(ssc,
      PreferConsistent,
      Subscribe[Long,String](topicsSet,kafkaParams))
    val HeartBeatDSm : org.apache.spark.streaming.dstream.DStream[(String,HeartBeatCs)] = stream
      .map( record =>{
        val v = parse(record.value)
        (v.appname, v)
      })

    val updateState = (appname: String, heartbeat:Option[HeartBeatCs], state: State[Long]) => {

      val defaultHeartBeatCs= HeartBeatCs("",  new Date().getTime())
      val prevHeartBeat: Option[Long] = state.getOption()
      val currentHeartbeat: HeartBeatCs = heartbeat.getOrElse(defaultHeartBeatCs)
      (prevHeartBeat, currentHeartbeat) match {
        case (None,currentHeartbeat) => {
          state.update(currentHeartbeat.adate)
          Some((currentHeartbeat.appname, currentHeartbeat, currentHeartbeat.adate))
        }

        case (Some(prevHeartBeat),currentHeartbeat) => {
          if (currentHeartbeat.adate-prevHeartBeat>15000){
            println("L'application " + appname + " N'a pas envoyer de données depuis " +(currentHeartbeat.adate-prevHeartBeat))
          }

          state.update(currentHeartbeat.adate)
          Some((currentHeartbeat.appname, "Missing Signal"))
        }

      }

    }
    val spec = StateSpec.function(updateState)
    val mappedStatefulStream = HeartBeatDSm.mapWithState(spec)

    mappedStatefulStream.print()

    //    mappedStatefulStream.foreachRDD(rdd => {
    //          if (!rdd.isEmpty()){
    //            rdd.saveToCassandra("transactionks", "transactiontb")
    //          }
    //        })

    ssc.start() // start the streaming context
    ssc.awaitTermination() // block while the context is running (until it's stopped by the timer)
    ssc.stop() // this additional stop seems to be required
  }

}
