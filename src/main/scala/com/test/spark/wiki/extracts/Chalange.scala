package com.test.spark.wiki.extracts
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.Row

object Chalange {
  val conf = new SparkConf().setMaster("local[*]").setAppName("textfileWriter")
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config(conf)
    .getOrCreate()


  //conf.setMaster("local")


  // Using above configuration to define our SparkContext

//  val sc = new SparkContext(conf)


  // Defining SQL context to run Spark SQL

  // It uses library org.apache.spark.sql._

//  val sqlContext = new SQLContext(sc)


  def main(args: Array[String]): Unit = {
    println("HELOOOOOOOOOOOOOOOOOOOOOOO")
    val squidString = spark.sparkContext.textFile("resources\\textRDD")
    println(squidString.getClass)


    // Defining the data-frame header structure

    val squidHeader = "time duration client_add result_code bytes req_method url user hierarchy_code type"
    val schema = StructType(squidHeader.split(" ").map(fieldName => {
      fieldName match {
        case "time" => StructField(fieldName,DoubleType, true)
        case _ => StructField(fieldName,StringType, true)
      }
    }))
    println("SHEMAAAAA  ",schema)
    val rowRDD = squidString.map(_.split(" ")).map(x =>
      {
        Row(x(0), x(1), x(2), x(3), x(4), x(5) , x(6) , x(7) , x(8), x(9))
      })

    println("ROWWW  ",rowRDD)


    // Creating dataframe based on Row RDD and schema
    val sqDF = spark.createDataFrame(rowRDD,schema)
    //sqDF.show()


//    val squidDF = sqlContext.createDataFrame(rowRDD, schema)
//    println(squidDF)
    sqDF

      .repartition(1)

      .write

      .mode ("overwrite")

      .format("com.databricks.spark.csv")

      .option("header", "true")

      .save("src\\main\\resources\\targetfile.csv")


  }

}
