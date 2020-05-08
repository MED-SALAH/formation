package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession

object PortfolioAPP {
//  implicit val spark: SparkSession = SparkSession
//    .builder()
//    .master("local[*]")
//    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val filePath = "C:\\www\\formation2salah\\formation\\src\\main\\resources\\deals.csv"
    val filePath2 = "C:\\www\\formation2salah\\formation\\src\\main\\resources\\deals2.csv"

      //val myDs =
//    val myDf = PortfolioService.readCSV(filePath2).rdd
    PortfolioService.readCSV(filePath2)

//    val ds = PortfolioService.nullcsv(filePath2)
//    ds.show()

//    //PortfolioService.getSumDeals(ds).show()
//    PortfolioService.getSumDeals(ds).show()

  }

}
