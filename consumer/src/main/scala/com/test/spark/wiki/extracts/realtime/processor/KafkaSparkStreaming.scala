package com.test.spark.wiki.extracts.realtime.processor
import java.util.Date

import com.datastax.spark.connector.writer.SqlRowWriter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import com.test.spark.wiki.extracts.domains.{AccountType, TX, Transaction}
import org.apache.spark.sql.{Encoders, SQLContext, SparkSession}
import org.apache.spark.streaming.{State, StateSpec, Time}


object KafkaSparkStreaming {

  val BOOTSTRAP_SERVERS = "ec2-15-188-51-222.eu-west-3.compute.amazonaws.com:9092"
  val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)

  def parse(json:String):TX = {
    objectMapper.readValue(json, classOf[TX])
  }

  def main(args: Array[String]): Unit = {

    implicit val  encoder = Encoders.product[TX]

    // read the configuration file
    val sparkConf = new SparkConf().setAppName("Transaction")
    sparkConf.setIfMissing("spark.master", "local[*]")
    sparkConf.set("spark.cassandra.connection.host", "35.180.46.40")
    sparkConf.set("spark.streaming.backpressure.enabled", "true")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Milliseconds(500))
    ssc.checkpoint("./chekpoint")




    println(s"Starting....Listen to ${BOOTSTRAP_SERVERS}")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> BOOTSTRAP_SERVERS,
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
    val transactionDSm: org.apache.spark.streaming.dstream.DStream[(AccountType, TX)] = stream
      .map( record =>{
        val tx = parse(record.value)
        val accounttype = AccountType(tx.account, tx.atype)
        (accounttype, tx)
      })


    val updateState = (accounttype: AccountType, tx: Option[TX], state: State[TX]) => {



      val defaultTX = TX("-1", accounttype.atype, new Date().getTime(),  accounttype.account, Double.MinValue)

      val prevMax: Option[TX] = state.getOption()

      val transactionMax: TX = tx.getOrElse(defaultTX)

      (prevMax, transactionMax) match {
        case (None, max) => {
          println(s">>> --------------------------------- <<<")
          println(s">>> key       = $accounttype")
          println(s">>> value     = $tx")
          println(s">>> state     = $state")
          println(s">>> MAX     = $max")
          state.update(max)
          Some((accounttype, tx, max)) // mapped value
        }

        case (Some(prevMax), currentMax) => {
          if(prevMax.amount < currentMax.amount){
            println(s">>> --------------------------------- <<<")
            println(s">>> key       = $accounttype")
            println(s">>> value     = $tx")
            println(s">>> state     = $state")
            println(s">>> MAX     = $currentMax")
            state.update(currentMax)
            Some((accounttype, tx, currentMax)) // mapped value
          }
        }
      }

    }
    val spec = StateSpec.function(updateState)
    val mappedStatefulStream = transactionDSm.mapWithState(spec)

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
