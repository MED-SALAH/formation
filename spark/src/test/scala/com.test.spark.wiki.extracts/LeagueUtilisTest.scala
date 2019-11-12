package com.test.spark.wiki.extracts

import com.test.spark.wiki.extracts.processors.LeagueProcess
import com.test.spark.wiki.extracts.domains.League
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FlatSpec

class LeagueUtilisTest extends FlatSpec{
  implicit  val  spark: SparkSession=SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  "convert Seq to Ds when Sequence Not empty file" should "OK" in {
    //Given
    val seqleag:Seq[League] = Seq(League("name","url"))
    val expected = Array(League("name","url"))

    //When
    val result:Dataset[League] = LeagueProcess.convert(seqleag)
    println("le Resultat ===", result)
    println("le Expected ===", expected)
    //Then
    assert(result.collect().sameElements(expected))

  }

}
