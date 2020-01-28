package com.test.spark.wiki.extracts

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SQLContext, SparkSession}

import scala.io.Source
import Implicit._

object RDDexample {
  implicit val spark: SparkSession=SparkSession.builder().master("local[*]").getOrCreate()
//  val conf = new SparkConf()
//  val sc = new SparkContext(conf)
//  val sqlContext = new SQLContext(sc)

  def parserline(line:String)={
    val fields = line.split(" ")
    val ele1 = fields(0).toString
    (ele1)
  }

  def readTextFile(filePath:String,header:String)(implicit spark:SparkSession)={
    //val filtext = sc.textFile(filePath)
    val filtext = spark.read.textFile(filePath)
//    val txt = sc.textFile(filePath).map(p =>{
//      p.split(" ")
//    })
    //val rdd = filtext.map(f => parserline(f)
    //println(rdd)
    //println(filtext)

    //println(Source.fromFile(filePath).map(p => p.split(" ")))

  }
}
