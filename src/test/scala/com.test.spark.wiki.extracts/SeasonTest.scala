package com.test.spark.wiki.extracts

import com.test.spark.wiki.extracts.processors.{LeagueProcess, SeasonProcess}
import com.test.spark.wiki.extracts.domains.{League, Season}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FlatSpec

class SeasonTest extends FlatSpec{
  implicit  val  spark: SparkSession=SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  "convert Dataset league to season" should "OK" in {
    //Given
    val leagueDs:Dataset[League] = LeagueProcess.convert(Seq(League("league1","https://example")))
    val expected = Seq(Season("league1","https://example",2018),Season("league1","https://example",2019))
    //when
    val result:Dataset[Season] = SeasonProcess.convertoseason(leagueDs,y1=2018,y2=2019)

    //then
    assert(result.collect().toList == expected)
  }
  "Récupérer les seasons d'une league " should "OK" in {
    //Give
    val league = League("league1","https://fr.wiki_%s-%s")
    val datedebut = 2018
    val datefin = 2019
    val saison2018 = Season("league1","https://fr.wiki_2018-2019",year=2018)
    val saison2019 = Season("league1","https://fr.wiki_2019-2020",year=2019)
    val expected = Seq(saison2018,saison2019)

    //When
    val result:Seq[Season] = SeasonProcess.leagtoSeason(league,datedebut,datefin)

    //Then
    assert(result==expected)


  }

}
