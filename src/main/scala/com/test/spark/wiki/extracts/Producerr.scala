package com.test.spark.wiki.extracts
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConverters._
import java.util.{Date, Properties}
import org.apache.log4j._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object Producerr {
  def main(args: Array[String]): Unit = {
    val events = 1000
//    val topic = args(1)
//    val brokers = args(2)
    val rnd = new Random()
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("client.id", "ScalaProducerExample")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    val producer = new KafkaProducer[String, String](props)
    val t = System.currentTimeMillis()
    for (nEvents <- Range(0, events)) {
      val runtime = new Date().getTime()
      val ip = "192.168.2." + rnd.nextInt(255)
      val msg = runtime + "," + nEvents + ",www.example.com," + ip
      val data = new ProducerRecord[String, String]("Transaction", ip, msg)

      //async
      //producer.send(data, (m,e) => {})
      //sync
      producer.send(data)
    }

    System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
    producer.close()
}
}