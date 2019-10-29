package com.test.spark.wiki.extracts

import org.scalatest.FlatSpec
import ImpliciteBigapps._
import org.apache.spark.sql.{DataFrame, Dataset}

class StatTestSpark extends FlatSpec{
  "La moyenne de butes" should "OK" in {
    //Given
    val season1 = Season("League1","https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_2017-2018",year=2017)
    val season2 = Season("League1","https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_2018-2019",year=2018)

    val avg1 = AvrageGols(season1,51.650000000000006)
    val avg2 = AvrageGols(season2,48.6)
    val listSea = spark.createDataset(Seq(season1,season2))
    val expected = Array(avg1,avg2)
    //Then
    val result:Dataset[AvrageGols] = StatUtilisSpark.getAverageGoalsbySeason(listSea)
    println("Teste 1 - the expected",expected)
    println("Teste 1 - the result",result)
    //When
    assert(expected.sameElements(result.collect()))
  }
  "Get date creation" should "OK" in{
    //Given
    val ligue = League("League 1","https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_%s-%s")
    val created = 1932
    val expected = created
    //When
    val result:Int = StatUtilisSpark.getCreatedYear(ligue)
    println("Teste 2 - the expected",expected)
    println("Teste 2 - the result",result)
    //Then
    assert(expected==result)
  }

  "Get Moste Title Team" should "OK" in{
    //Given
    val ligue20171 = LeagueStanding("Ligue 1",2017,1,"Paris Saint-Germain",93,38,29,6,3,108,29,79)
    val ligue20172 = LeagueStanding("Ligue 1",2017,2,"AS Monaco",80,38,24,8,6,85,45,40)
    val ligue20181 = LeagueStanding("Ligue 1",2018,1,"Paris Saint-Germain",91,38,29,4,5,105,35,70)
    val ligue20182 = LeagueStanding("Ligue 1",2018,2,"LOSC",75,38,22,9,7,68,33,35)
    val ligueA20171 = LeagueStanding("LigueA",2017,1,"FC Barcelone",93,38,28,9,1,99,29,70)
    val ligueA20172 = LeagueStanding("LigueA",2017,2,"Atlético de Madrid",79,38,23,10,5,58,22,36)
    val ligueA20181 = LeagueStanding("LigueA",2018,1,"FC Barcelone",87,38,26,9,3,90,36,54)
    val ligueA20182 = LeagueStanding("LigueA",2018,2,"Atlético de Madrid",76,38,22,10,6,55,29,26)

    val leaguestandings = spark.createDataset(Seq(ligue20171,ligue20172,ligue20181,ligue20182,ligueA20171,ligueA20172,ligueA20181,ligueA20182))
    val re1 = MostTitleTeamSpark("Ligue 1","Paris Saint-Germain")
    val re2 = MostTitleTeamSpark("LigueA","FC Barcelone")
    val expected = Array(re1,re2)

    //When
    val result:Dataset[MostTitleTeamSpark] = StatUtilisSpark.getMostTitelTeam(leaguestandings)
    println("Teste 3 - the expected",expected)
    println("Teste 3 - the result",result.collectAsList())
    //Then
    assert(expected.sameElements(result.collect()))
  }
  "Get Average Point for Winner" should "OK" in{
    //Given
    val ligue20171 = LeagueStanding("Ligue 1",2017,1,"Paris Saint-Germain",93,38,29,6,3,108,29,79)
    val ligue20172 = LeagueStanding("Ligue 1",2017,2,"AS Monaco",80,38,24,8,6,85,45,40)
    val ligue20181 = LeagueStanding("Ligue 1",2018,1,"Paris Saint-Germain",91,38,29,4,5,105,35,70)
    val ligue20182 = LeagueStanding("Ligue 1",2018,2,"LOSC",75,38,22,9,7,68,33,35)
    val ligueA20171 = LeagueStanding("LigueA",2017,1,"FC Barcelone",93,38,28,9,1,99,29,70)
    val ligueA20172 = LeagueStanding("LigueA",2017,2,"Atlético de Madrid",79,38,23,10,5,58,22,36)
    val ligueA20181 = LeagueStanding("LigueA",2018,1,"FC Barcelone",87,38,26,9,3,90,36,54)
    val ligueA20182 = LeagueStanding("LigueA",2018,2,"Atlético de Madrid",76,38,22,10,6,55,29,26)

    val leaguestandings = spark.createDataset(Seq(ligue20171,ligue20172,ligue20181,ligue20182,ligueA20171,ligueA20172,ligueA20181,ligueA20182))
    val re1 = AveragePointWinnerSpark("Ligue 1",92.0)
    val re2 = AveragePointWinnerSpark("LigueA",90.0)
    //val re3 = AveragePointWinnerSpark("ligue3",91.6)
    val expected = Array(re1,re2).toList
    //When
    val result:Dataset[AveragePointWinnerSpark] = StatUtilisSpark.getAveragePointWinner(leaguestandings)
    println("Teste 4 - the expected",expected.toList)
    println("Teste 4 - the result",result.collect().toList)
    //Then
    assert(expected.sameElements(result.collect().toList))
  }

  "ce test filtere sur la 1ere et 2eme equipe par saison " should "OK" in {
    //Given
    val leagueStandingDS=getDataSetLeagueDS()
    val leagueStanding1 = LeagueStanding("Ligue 1",2017,1,"Paris Saint-Germain",93,38,29,6,3,108,29,79)
    val leagueStanding2 = LeagueStanding("Ligue 1",2017,2,"AS Monaco",80,38,24,8,6,85,45,40)
    val leagueStanding4 = LeagueStanding("Ligue 1",2018,1,"Paris Saint-Germain",91,38,29,4,5,105,35,70)
    val leagueStanding5 = LeagueStanding("Ligue 1",2018,2,"LOSC",75,38,22,9,7,68,33,35)

    val leagueStanding7 = LeagueStanding("Liga",2017,1,"FC Barcelone",93,38,28,9,1,99,29,70)
    val leagueStanding8 = LeagueStanding("Liga",2017,2,"Atlético de Madrid",79,38,23,10,5,58,22,36)

    val leagueStanding10 = LeagueStanding("Liga",2018,1,"FC Barcelone",87,38,26,9,3,105,36,54)
    val leagueStanding11 = LeagueStanding("Liga",2018,2,"Atlético de Madrid",76,38,22,10,6,105,29,26)
    val expected = spark.createDataset(Seq(leagueStanding1,leagueStanding2,leagueStanding4,leagueStanding5,
      leagueStanding7,leagueStanding8,leagueStanding10,leagueStanding11))
    //When
    val result:Dataset[LeagueStanding] = StatUtilisSpark.getFirstSecondLs(leagueStandingDS)

    result.show()

    //Then

    assert(result.collect().sameElements(expected.collect()))
  }
  "ce test permet de grouper par league et season les leagueStanding et calcule la difference de points" should "OK" in {
    //Given
    val leagueStanding1 = LeagueStanding("Ligue 1",2017,1,"Paris Saint-Germain",93,38,29,6,3,108,29,79)
    val leagueStanding2 = LeagueStanding("Ligue 1",2017,2,"AS Monaco",80,38,24,8,6,85,45,40)
    val leagueStanding3 = LeagueStanding("Ligue 1",2017,3,"Olympique lyonnais",78,38,23,9,6,87,43,44)

    val leagueStanding4 = LeagueStanding("Ligue 1",2018,1,"Paris Saint-Germain",91,38,29,4,5,105,35,70)
    val leagueStanding5 = LeagueStanding("Ligue 1",2018,2,"LOSC",75,38,22,9,7,68,33,35)
    val leagueStanding6 = LeagueStanding("Ligue 1",2018,3,"lympique lyonnais",72,38,21,9,8,70,47,23)

    val leagueStanding7 = LeagueStanding("Liga",2017,1,"FC Barcelone",93,38,28,9,1,99,29,70)
    val leagueStanding8 = LeagueStanding("Liga",2017,2,"Atlético de Madrid",79,38,23,10,5,58,22,36)
    val leagueStanding9 = LeagueStanding("Liga",2017,3,"Real Madrid",76,38,22,10,6,94,44,50)

    val leagueStanding10 = LeagueStanding("Liga",2018,1,"FC Barcelone",87,38,26,9,3,105,36,54)
    val leagueStanding11 = LeagueStanding("Liga",2018,2,"Atlético de Madrid",76,38,22,10,6,105,29,26)
    val leagueStanding12 = LeagueStanding("Liga",2018,3,"Raal Madrid",68,38,21,5,12,63,46,17)
    val leagueStandingDS = getDataSetLeagueDS()
    val expected  = Seq(("League 1",13),("Liga",11))

    //When
    val result:DataFrame = StatUtilisSpark.groupByLs(leagueStandingDS)

    result.show()

    //Then

    assert(result.collect().toList == expected.toList)
  }

  "ce test permet de calculer la difference de points entre la premiere et la deuxieme equipe du championnat" should "ok" in {

  }

  "ce test permet de retourner la saison la plus serée d'une ligue" should "ok" in {

  }
  "calcule de difference entre des listes" should "OK" in {
    //Given
    val leagueStanding1 = LeagueStanding("Ligue 1",2017,1,"Paris Saint-Germain",93,38,29,6,3,108,29,79)
    val leagueStanding2 = LeagueStanding("Ligue 1",2017,2,"AS Monaco",80,38,24,8,6,85,45,40)
    val leagueStanding3 = LeagueStanding("Ligue 1",2017,3,"Olympique lyonnais",78,38,23,9,6,87,43,44)
    val leagueStandingLS = spark.createDataset(List(leagueStanding1,leagueStanding2,leagueStanding3))
    val expected = List(15,leagueStandingLS)
    //When
    val result:DataFrame = StatUtilisSpark.groupByLs(leagueStandingLS)
    //Then
    assert(expected.collect().sameElements(result.collect().toList))
  }

  def getDataSetLeagueDS():Dataset[LeagueStanding]={
    val leagueStanding1 = LeagueStanding("Ligue 1",2017,1,"Paris Saint-Germain",93,38,29,6,3,108,29,79)
    val leagueStanding2 = LeagueStanding("Ligue 1",2017,2,"AS Monaco",80,38,24,8,6,85,45,40)
    val leagueStanding3 = LeagueStanding("Ligue 1",2017,3,"Olympique lyonnais",78,38,23,9,6,87,43,44)

    val leagueStanding4 = LeagueStanding("Ligue 1",2018,1,"Paris Saint-Germain",91,38,29,4,5,105,35,70)
    val leagueStanding5 = LeagueStanding("Ligue 1",2018,2,"LOSC",75,38,22,9,7,68,33,35)
    val leagueStanding6 = LeagueStanding("Ligue 1",2018,3,"lympique lyonnais",72,38,21,9,8,70,47,23)

    val leagueStanding7 = LeagueStanding("Liga",2017,1,"FC Barcelone",93,38,28,9,1,99,29,70)
    val leagueStanding8 = LeagueStanding("Liga",2017,2,"Atlético de Madrid",79,38,23,10,5,58,22,36)
    val leagueStanding9 = LeagueStanding("Liga",2017,3,"Real Madrid",76,38,22,10,6,94,44,50)

    val leagueStanding10 = LeagueStanding("Liga",2018,1,"FC Barcelone",87,38,26,9,3,105,36,54)
    val leagueStanding11 = LeagueStanding("Liga",2018,2,"Atlético de Madrid",76,38,22,10,6,105,29,26)
    val leagueStanding12 = LeagueStanding("Liga",2018,3,"Raal Madrid",68,38,21,5,12,63,46,17)
    spark.createDataset(Seq(leagueStanding1,leagueStanding3,leagueStanding2,
      leagueStanding4,leagueStanding5,leagueStanding6,leagueStanding7,leagueStanding8,leagueStanding9,
      leagueStanding10,leagueStanding11,leagueStanding12))
  }

}
