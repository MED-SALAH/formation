//package kafka
//
//import java.util.Properties
//
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import io.confluent.kafka.serializers.KafkaAvroSerializer
//import org.apache.kafka.clients.producer.ProducerConfig
//import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
//import org.apache.kafka.clients.producer.KafkaProducer
//import org.apache.kafka.clients.producer.ProducerRecord
//import org.apache.kafka.common.serialization.StringSerializer
//import io.confluent.kafka.serializers.KafkaAvroSerializer
//
//
//object Producer2 {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[*]").setAppName("producer")
//    val ssc = new StreamingContext(conf, Seconds(1))
//
//
//    val props:Properties = new Properties()
////    props.put("bootstrap.servers","192.168.1.57:9092")
////    props.put("key.serializer","org.apache.kafka.common.serialization.LongSerializer")
////    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
////    props.put("acks","all")
//
//    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.57:9092")
//    props.put(ProducerConfig.ACKS_CONFIG, "all")
//    props.put(ProducerConfig.RETRIES_CONFIG, 0)
//    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
//    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
//    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://192.168.1.57:8081")
//
//    val topic = "Transaction"
//
//    try {
//      val producer = new KafkaProducer[String, Payment](props)
//      for (i <- 0 to 500) {
//          val orderId = "id" + Long.toString(i)
//          val payment = new Payment(orderId, 1000.00d)
//          val record = new ProducerRecord[String, Nothing](topic, payment.getId.toString, payment)
//          producer.send(record)
//          Thread.sleep(1000L)
//        }
//        producer.flush()
//        System.out.printf("Successfully produced 10 messages to a topic called %s%n", topic)
//      }catch{
//        case e:Exception => e.printStackTrace()
//      }finally {
//        producer.close()
//      }
//
//
//    //    ssc.start()
//    //    ssc.awaitTermination()// Start the computation
//
//
//
//  }
//
//
//}