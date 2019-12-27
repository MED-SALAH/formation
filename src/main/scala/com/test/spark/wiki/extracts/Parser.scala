package com.test.spark.wiki.extracts

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import com.test.spark.wiki.extracts.Implicit._
import org.apache.spark.sql.types.IntegerType
object Parser {
//  implicit val spark: SparkSession=SparkSession
//    .builder()
//    .master("local[*]")
//    .getOrCreate()

def read(FilePath:String)(implicit spark: SparkSession): Dataset[Faits] = {

  val ds = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(FilePath).as[Faits]

  ds.filter(_.Id=="1")
  //data.filter(_.Energy == 117 )
  //data.show()
}

}
