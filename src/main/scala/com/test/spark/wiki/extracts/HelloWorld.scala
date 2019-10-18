package com.test.spark.wiki.extracts


import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object  HelloWorld {

  def main(args: Array[String]): Unit = {

    implicit  val  spark: SparkSession=SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    println("Hello World")
  }


}