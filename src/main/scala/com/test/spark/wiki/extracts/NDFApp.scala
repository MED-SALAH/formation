package com.test.spark.wiki.extracts

import org.apache.spark.sql.{SaveMode, SparkSession}
import Implicit._

object NDFApp {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    val ndfPath = "C:\\www\\formation2salah\\formation\\src\\main\\resources\\top50.csv"
    val resuPathParquet = "C:\\www\\formation2salah\\formation\\src\\main\\resources\\resu.parquet"
    val ndfPathParquet = "C:\\www\\formation2salah\\formation\\src\\main\\resources\\top50.parquet"

    val myDs = NDFservice.readFile(ndfPath)
    myDs.write.mode(SaveMode.Overwrite).parquet(ndfPathParquet)
    val myParquet = spark.read.parquet(ndfPathParquet)
    myParquet.as[Faits].show()

    NDFservice.getTotalInergi(myDs)
    //NDFservice.getTotalInergi(myParquet.as[Faits])
    val res = NDFservice.getTotalby(myDs)
    res.write.mode(SaveMode.Overwrite).parquet(resuPathParquet)
    res.printSchema()
  }

}
