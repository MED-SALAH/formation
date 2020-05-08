package kafka
import org.apache.spark.sql.SparkSession

object ConsumerAvro {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SimpleConsumer")
      .master("local[*]")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "http://127.0.0.1:9092")
      .option("subscribe", "pageviews2")
      .load()

    df
      .writeStream
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()

  }
}
