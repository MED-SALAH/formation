package com.test.spark.wiki.extracts.utils

import com.test.spark.wiki.extracts.utils.Parser.convertListToDF
import cucumber.api.DataTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.JavaConversions._


object ParserUtils {


  def parseDataTable(table: DataTable)(implicit spark: SparkSession): DataFrame = {
    val dataTableAsListOfList:List[java.util.List[String]] = table.raw().toList
    convertListToDF(dataTableAsListOfList)
  }

}
