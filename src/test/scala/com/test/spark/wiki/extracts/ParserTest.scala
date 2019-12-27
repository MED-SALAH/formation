package com.test.spark.wiki.extracts

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.FlatSpec

class ParserTest extends FlatSpec {
  implicit val spark: SparkSession=SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  "parser file csv " should "OK" in {
    //Given
    val FilePath = "C:\\www\\formation2salah\\formation\\src\\main\\resources\\top50.csv"

    val expected = List(1,"Se�orita","Shawn Mendes","canadian pop",117,55,76,-6,8,75,191,4,3,79)

    //When
    val result:Dataset[Faits] = Parser.read(FilePath)
    println(result.collect().toList)
    //Then
    //assert(result.collect().sameElements(expected.collect()))




  }

}
