package com.test.spark.wiki.extracts.utils

import com.test.spark.wiki.extracts.domains.League
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.scalatest.{FlatSpec, FunSuite}
import java.util.{List => JList}

import scala.collection.JavaConversions._
import collection.JavaConverters._
import scala.collection.immutable._

class ParserTest extends FlatSpec {

  implicit  val  spark: SparkSession=SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  "parser le header d'une datatable" should "OK" in {
    //Given
    val headerList:List[String] = List("name:String", "age:Integer", "adress:String", "CasPasSchema")
    val expectedSchema: StructType = StructType(List(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("adress", StringType, true),
      StructField("CasPasSchema", StringType, true)

    ))

    //when
    val resultSchema:StructType = Parser.schema(headerList)

    //Then
    assert(expectedSchema==resultSchema)
  }



  "parser les data et convertir en Dataframe" should "OK" in {
    //Given
    val headerList:JList[String] = List("name:String", "age:Integer").asJava

    val firstRow:JList[String] = List("A", "18").asJava
    val secondRow:JList[String] = List("B", "28").asJava
    val dataTail =  List(headerList, firstRow, secondRow)

    //when
    val resultDF:DataFrame = Parser.convertListToDF(dataTail)
    resultDF.show()
    //Then
  }


  "parser un fichier schema csv pour recuperer le schema" should "OK" in {
    //Given
    val schemaString = "league;season:Integer"

    val expectedSchema: StructType = StructType(List(
      StructField("league", StringType, true),
      StructField("season", IntegerType, true)
    ))

         //when
    val schema:StructType = Parser.readSchemaFromHeader(schemaString)

    println("expected  ==> " + expectedSchema)
    println("schema  ==> " + schema)
    //Then

    assert( schema == expectedSchema)


  }


}
