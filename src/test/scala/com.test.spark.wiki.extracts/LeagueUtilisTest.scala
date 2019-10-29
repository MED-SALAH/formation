package com.test.spark.wiki.extracts

import org.apache.spark.sql.Dataset
import org.scalatest.FlatSpec

class LeagueUtilisTest extends FlatSpec{
  import ImpliciteBigapps._
  "convert Seq to Ds when Sequence Not empty file" should "OK" in {
    //Given
    val seqleag:Seq[League] = Seq(League("name","url"))
    val expected = Array(League("name","url"))

    //When
    val result:Dataset[League] = LeagueUtils.convert(seqleag)
    println("le Resultat ===", result)
    println("le Expected ===", expected)
    //Then
    assert(result.collect().sameElements(expected))

  }

}
