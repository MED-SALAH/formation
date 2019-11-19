package com.test.spark.wiki.extracts.utils

import java.util.{List => JList}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.test.spark.wiki.extracts.domains.League
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConversions._


object Parser {
  def readSchemaFromHeader(header: String): StructType = {
    val schemaListString: List[String] = header.split(";").toList
    schema(schemaListString)
  }

  def schema(headerList: List[String]): StructType = {

    val fields:List[StructField] =  headerList.map(elm=>{
      val splitted: Array[String] = elm.split(":")
      val name =  splitted(0)
      val dataType:DataType =  splitted.size match {
        case 0 => StringType
        case 1 => StringType
        case _ => {
          val dataTypeString = splitted(1)
          dataTypeString.toLowerCase match {
            case "string" => StringType
            case "integer" => IntegerType
            case "boolean" => BooleanType
            case "double" => DoubleType
            case _ => StringType
          }
        }
      }


      StructField(name, dataType, true)
    })

    StructType(fields)


  }

  def parse(path:String): Seq[League] ={
    path match {
      case  "" =>
        Seq.empty
      case _  =>
        val mapper = new ObjectMapper(new YAMLFactory())
        mapper.registerModule(DefaultScalaModule)
        val inputStream = getClass().getClassLoader().getResourceAsStream(path)
        mapper.readValue(inputStream, classOf[Array[League]]).toSeq
    }
  }

  def convertListToDF(dataTableAsListOfList:List[JList[String]])(implicit spark: SparkSession):DataFrame = {
    val headerList:List[String] = dataTableAsListOfList.get(0).toList
    val scheam = schema(headerList)

    val dataTail:List[JList[String]] = dataTableAsListOfList.tail

    val data:List[Row] = dataTail.map{
      case aLine:JList[String] => {

        val dataSeq:Seq[Any] =  aLine.toList.zipWithIndex.map {
          case(elt, index) => {
            val field: StructField = scheam.fields.toList.get(index)
            field.dataType match {
              case IntegerType => elt.toInt
              case BooleanType => elt.toBoolean
              case _ => elt
            }
          }
        }

        Row.fromSeq(dataSeq)
      }
    }

    spark.createDataFrame(data, scheam)
  }
}
