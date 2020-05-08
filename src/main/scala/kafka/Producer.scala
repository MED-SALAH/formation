package kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Producer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("producer")
//    val ssc = new StreamingContext(conf, Seconds(1))


    val props:Properties = new Properties()
    props.put("bootstrap.servers","35.180.127.210:9092")
    props.put("key.serializer","org.apache.kafka.common.serialization.LongSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")

    val producer = new KafkaProducer[Long, String](props)
    val topic = "test2"

    // create a timer that we will use to stop the processing after 60 seconds so we can print some results
    try {
      for (i <- 0 to 500) {
        val record = new ProducerRecord[Long, String](topic, i, "bonjour je suis salah bigapps" + i)
        producer.send(record)
        //println(record.topic(),record.key(),record.value(),metadata.get().timestamp(),metadata.get().partition())
        //Thread.sleep(5000)
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      producer.close()
    }

//    ssc.start()
//    ssc.awaitTermination()// Start the computation



  }

}
