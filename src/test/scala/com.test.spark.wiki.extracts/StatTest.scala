package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class StatTest extends FlatSpec{

  implicit  val  spark: SparkSession=SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  "la moyenne des butes" should "OK" in {
    //Given
    val season1 = Season("League1","https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_2017-2018",year=2017)
    val season2 = Season("League1","https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_2018-2019",year=2018)
    val avg1 = AvrageGols(season1,51.650000000000006)
    val avg2 = AvrageGols(season2,48.6)
    val expected = Seq(avg1,avg2)


    //When
    val result:Seq[AvrageGols] = StatUtils.getAverageGoalsbySeason(Seq(season1,season2))
    println("Teste 1 - the expected",expected)
    println("Teste 1 - the result",result)
    //Then
    assert(expected==result)

  }
  "Get date creation" should "OK" in{
    //Given
    val ligue = League("League 1","https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_%s-%s")
    val created = 1932
    val expected = created
    //When
    val result:Int = StatUtils.getCreatedYear(ligue)
    println("Teste 2 - the expected",expected)
    println("Teste 2 - the result",result)
    //Then
    assert(expected==result)


  }

  "Get Moste Title Team" should "OK" in{
    //Given
    val ligue1 = League("League 1","https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_%s-%s")
    val ligue2 = League("League A","https://fr.wikipedia.org/wiki/Championnat_d'Italie_de_football_%s-%s")
    val re1 = MostTitleTeam(ligue1,"Paris Saint-Germain")
    val re2 = MostTitleTeam(ligue2,"Juventus FC")
    val expected = Seq(re1,re2)

    //When
    val result:Seq[MostTitleTeam] = StatUtils.getMostTitelTeam(Seq(ligue1,ligue2))
    println("Teste 3 - the expected",expected)
    println("Teste 3 - the result",result)
    //Then
    assert(expected==result)
  }
  "Get Average Point for Winner" should "OK" in{
    //Given
    val ligue1 = League("League 1","https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_%s-%s")
    val ligue2 = League("League A","https://fr.wikipedia.org/wiki/Championnat_d'Italie_de_football_%s-%s")
    val ligue3 = League("Liga","https://fr.wikipedia.org/wiki/Championnat_d'Espagne_de_football_%s-%s")
    val re1 = AveragePointWinner(ligue1,91.60000000000001)
    val re2 = AveragePointWinner(ligue2,90.8)
    val re3 = AveragePointWinner(ligue3,91.6)
    val expected = Seq(re1,re2,re3)
    //When
    val result:Seq[AveragePointWinner] = StatUtils.getAveragePointWinner(Seq(ligue1,ligue2,ligue3))
    println("Teste 4 - the expected",expected)
    println("Teste 4 - the result",result)
    //Then
    assert(expected==result)
  }

}
