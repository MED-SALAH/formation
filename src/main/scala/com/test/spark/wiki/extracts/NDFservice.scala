package com.test.spark.wiki.extracts

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions._
import Implicit._
import org.apache.spark.sql.expressions.Window

object NDFservice {
  implicit val spark : SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

  def readFile(filePath:String):Dataset[Faits]={
    val myDs = spark.read
      .option("delimiter",",")
      .option("header","true")
      .option("inferSchema","true")
      .csv(filePath).as[Faits]
//    myDs.map(record => {
//      println(record.montant.toDouble)
//      NDF(record.dt_frais,record.montant.toDouble, record.description, record.path_justificatif, record.type_frais)
//    })
//      .filter(record => {
//
//        val result = Try(record.getString(1).toDouble)
//
//        result match {
//          case Success(value) => true
//          case Failure(exception) => false
//        }
//      })
//      .map(record =>{
//        println(record.getString(0),record.getDouble(1))
//        NDF(record.getString(0), record.getString(1).toDouble, record.getString(2), record.getString(3), record.getString(4))
//
//    })
    myDs.show()
    myDs

  }
  def getTotalInergi(myData:Dataset[Faits])={
    val total = myData.agg(sum(col("Energy")).as("sommeTotal"))
    total.show()
    total.printSchema()
    println(total.select(col("sommeTotal")).first.getLong(0))
    total.select(col("sommeTotal")).first.getLong(0)
  }

  def getTotalby(myData:Dataset[Faits])={
    val t = getTotalInergi(myData)
    val totals = myData.groupBy(col("Genre"))
      .agg(sum(col("Energy")).as("total"),mean(col("Energy")).as("mean")
        ,min(col("Energy")).as("min"),max(col("Energy")).as("max"))
      .withColumn("pourcentage_totale",((col("total")*100)/t))
        //.over(Window.orderBy("Genre")))
      .withColumn("intervale",col("max")-col("min"))
      .withColumn("Lag", lag("intervale",1,0)
        .over(Window.orderBy("intervale")))
      .withColumn("Delta",col("intervale")-col("Lag"))

    totals.show(49)
    totals.printSchema()
    totals
  }

}
