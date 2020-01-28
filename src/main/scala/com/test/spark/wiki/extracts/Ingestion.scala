package com.test.spark.wiki.extracts

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.test.spark.wiki.extracts.TestIngestion.getClass
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import Implicit._
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.xml.parsing.ConstructingParser

object Ingestion {
  implicit val spark: SparkSession=SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  def readfile(file:String):Seq[Flux]={
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    val inputStream = getClass().getClassLoader().getResourceAsStream(file)
    mapper.readValue(inputStream, classOf[Array[Flux]])

  }
  def readalert(file:String):Seq[Alert]={
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    val inputStream = getClass().getClassLoader().getResourceAsStream(file)
    mapper.readValue(inputStream, classOf[Array[Alert]])

  }

  def readcsvDS(file:String)(implicit spark:SparkSession):Dataset[Faits] = {
    spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(file).as[Faits]
  }

  def readSql(file:String)={
    val selectsql = Source.fromFile(file).mkString
    println(selectsql)
    spark.sql(selectsql)
  }


}
