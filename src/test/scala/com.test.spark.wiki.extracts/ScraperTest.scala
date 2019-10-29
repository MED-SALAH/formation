package com.test.spark.wiki.extracts

import org.scalatest.FlatSpec

class ScraperTest extends FlatSpec{
  "convert Dataset league to season" should "OK" in {
    //Given
    val season = Season("league1","https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_2017-2018",year=2017)
    //val urltest = "https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_2017-2018"
    val equipePSG = LeagueStanding("league1",2017,1,"Paris Saint-Germain",93,38,29,6,3,108,29,79)
    val expected = equipePSG
    //When
    val result:LeagueStanding = SeasonScraper.scraper(season).head
    println("le Resultat ===", result)
    println("le Expected ===", expected)
    //Then
    assert(expected==result)
  }

}
