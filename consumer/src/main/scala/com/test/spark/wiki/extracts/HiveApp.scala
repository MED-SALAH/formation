package com.test.spark.wiki.extracts

import com.datastax.spark.connector._
import com.test.spark.wiki.extracts.domains.ImpliciteBigapps._
import com.test.spark.wiki.extracts.processors.{LeagueProcess, SeasonProcess, StatProcess}
import com.test.spark.wiki.extracts.utils.{Parser, SeasonScraper}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object  HiveApp {


  def main(args: Array[String]): Unit = {
    implicit  val  spark: SparkSession=SparkSession
      .builder()
      .master("local[*]")
      .config("spark.cassandra.connection.host", "35.180.46.40")
      .getOrCreate()

    val headerFilePath = args(0)
    val dataFilePath = args(1)
    val reasulFilePath = args(2)

    println(s"headerFilePath => ${headerFilePath}")
    println(s"dataFilePath => ${dataFilePath}")
    println(s"reasulFilePath => ${reasulFilePath}")

    val header =  spark.read.textFile(headerFilePath).collect()(0)

    val schema:StructType = Parser.readSchemaFromHeader(header)

    val df = spark.read.schema(schema).option("delimiter", ";").csv(dataFilePath)

    df.printSchema()
    df.show()

    df.write.partitionBy("league", "season").parquet(reasulFilePath)

  }


}