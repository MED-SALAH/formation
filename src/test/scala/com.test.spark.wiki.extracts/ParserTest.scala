package com.test.spark.wiki.extracts
import org.scalatest.FlatSpec

class ParserTest extends FlatSpec{

  "parser leagues.yml when empty file" should "OK" in {
    //Given
    val leagueFilePath = ""
    val expected = Seq.empty
    //when
    val result:Seq[League] = Parser.parse(leagueFilePath)
    println("le Resultat 1 ===", result)
    println("le Expected 1 ===", expected)
    //Then
    assert(result==expected)
  }
  "parser leagues.yml when one league" should "OK" in {
    //Given
    val leagueFilePath = "league1.yaml"
    val ligue1 = League("Ligue 1","https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_%s-%s")
    val ligue2 = League("Ligue 2","https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_%s-%s")
    val expected = Seq(ligue1,ligue2)
    //when
    val result:Seq[League] = Parser.parse(leagueFilePath)
    println("le Resultat 2 ===", result)
    println("le Expected 2 ===", expected)
    //Then
    assert(result==expected)
  }


}
