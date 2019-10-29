package com.test.spark.wiki.extracts

import org.apache.spark.sql.Dataset
import org.scalatest.FlatSpec

class SeasonTest extends FlatSpec{
  import ImpliciteBigapps._

  "convert Dataset league to season" should "OK" in {
    //Given
    val leagueDs:Dataset[League] = LeagueUtils.convert(Seq(League("league1","https://example")))
    val expected = Array(Season("league1","https://example",2018))
    //when
    val result:Dataset[Season] = SeasonUtils.convertoseason(leagueDs,y1=2018,y2=2019)

    //then
    assert(result==expected)
  }
  "Récupérer les seasons d'une league " should "OK" in {
    //Give
    val league = League("league1","https://fr.wiki_%s-%s")
    val datedebut = 2018
    val datefin = 2019
    val saison2018 = Season("name","https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_%s-%s",year=2018)
    val saison2019 = Season("name","https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_%s-%s",year=2019)
    val expected = Seq(saison2018,saison2019)

    //When
    val result:Seq[Season] = SeasonUtils.leagtoSeason(league,datedebut,datefin)

    //Then
    assert(result.toArray==expected)


  }

}
