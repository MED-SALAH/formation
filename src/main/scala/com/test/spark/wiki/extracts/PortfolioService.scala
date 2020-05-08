package com.test.spark.wiki.extracts

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{when, _}
import Implicit._
import org.apache.spark.sql.expressions.Window
import org.apache.spark._
//import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object PortfolioService {
  implicit val spark : SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  def readCSV(filePath:String)={
    spark.sparkContext.textFile(filePath).map(f=>{
      println(f)
      f.split(";")
    })
//    spark.read.option("header","true")
//          //.option("inferShema","true")
//          .option("delimiter",";").csv(filePath)
  }

//  def nullcsv(filePath: String):Dataset[Portfolio]={
//    val myDs = readCSV(filePath)
//    myDs.map(elt => {
//      val result:Option[Double] = elt.getString(3) match {
//        case x if x == null => None
//        case a:String => Some(a.toDouble)
//      }
//      Portfolio(elt.getString(0),elt.getString(1),elt.getString(2),result)
//
//    })
//  }

//  def getSumDeals(myDs:Dataset[Portfolio]):Dataset[Deal]={
//    val test = myDs.withColumn("Genre",when(col("Type")==="pf","Parent")
//      .when(col("Type")==="dl","Fils")
//      .otherwise("ROOT")
//      .alias("new"))
////      .groupBy("PID")
////      .agg(collect_list("NID").as("fils")
////        ,count("NID").as("nbrFils")
////        ,sum("Val"))
//    test.show()
//
//    val parents = myDs.groupBy("PID","NID")
//      .agg(sum("Val").alias("csum"))
//      .orderBy("PID","NID")
//
//    parents.show()
//    val children = myDs.filter(elt => elt.Type =="dl").select("NID","PID","Val")
//    //.groupBy("NID").agg(sum().alias("csum"))
//    children.show()
//
//    //val vertexId = myDs.map(elt => elt.)
//    val myGraph: Graph[(String,Int),Int] = Graph(parents,children)
//
//
//
//
//
//    val re = parents
//      .select("NID")
//      .join(children, parents("NID") <=> children("PID"))
//      .select(col("PID"), col("csum").alias("holds"))
//    re.show()
//    //val test2 = myDs.filter(elt => elt.Type =="dl")
////    val fils3 = myDs.groupBy("PID","NID").agg(sum(col("Val") as "Val"))
////      //.rollup("NID","PID").sum()
////    println("flis3333")
////    fils3.show()
////    val fils4 = myDs.groupBy().agg(sum(col("Val") as "Val"))
////      .select(col("PID"), lit(null) as "NID",col("Val") )
////    println("flis4444444")
////    fils4.show()
////    val fils5 = myDs.groupBy("PID").agg(sum(col("Val") as "Val"))
////      .select(lit(null) as "PID", lit(null) as "NID",col("Val") )
////    println("fils555555")
////    fils5.show()
////    val qq = fils3
////      .union(fils4)
////      .union(fils5)
////      .sort(col("PID").desc_nulls_last, col("NID").asc_nulls_last)
////
////    println("totaaaaaal")
////    qq.show()
//
//    val fils2 = myDs.groupBy("PID")
//      .agg(collect_list("NID").as("fils")
//        ,count("NID").as("nbrFils")
//        ,sum("Val"))
////      .withColumn("SumEdge",sum("Val")
////        .over(Window.partitionBy("PID")))
//    fils2.show()
//
//    val fils = myDs.filter(elt => elt.Type =="dl")
//      .groupBy("PID")
//      .agg(sum(col("Val"))
//      .as("sum")).as[Deal]
//    val parent = myDs.filter(elt => {
//      val el = elt.Type match {
//        case x if x == "dl" => true
//        case _ => false
//      }
//      elt.PID != "root" &&
//      elt.PID != null &&
//      el
//    }).groupBy("PID","NID").agg(sum("Val"))
//    //val re = parent.joinWith()
//    parent.show()
//    fils
//
//
//  }

}
