package com.test.spark.wiki.extracts

import com.test.spark.wiki.extracts.domains.ImpliciteBigapps._
import com.test.spark.wiki.extracts.processors.StatProcess
import com.test.spark.wiki.extracts.domains.{AveragePointWinnerSpark, AvrageGols, League, LeagueMaxPoints, LeagueStanding, MostTitleTeamSpark, Season}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FlatSpec

class StatTestSpark extends FlatSpec{
  implicit  val  spark: SparkSession=SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  "La moyenne de butes" should "OK" in {
    //Given
    val season1 = Season("League1","https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_2017-2018",year=2017)
    val season2 = Season("League1","https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_2018-2019",year=2018)

    val avg1 = AvrageGols(season1,51.650000000000006)
    val avg2 = domains.AvrageGols(season2,48.6)
    val listSea = spark.createDataset(Seq(season1,season2))
    val expected = Array(avg1,avg2)
    //Then
    val result:Dataset[AvrageGols] = StatProcess.getAverageGoalsbySeason(listSea)
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
    val result:Int = StatProcess.getCreatedYear(ligue)
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

    val ligueA20171 = LeagueStanding("LigueA",2017,1,"FC Barcelone",87,38,26,9,3,90,36,54)
    val ligueA20172 = LeagueStanding("LigueA",2017,2,"Atlético de Madrid",79,38,23,10,5,58,22,36)
    val ligueA20181 = LeagueStanding("LigueA",2018,1,"FC Barcelone",87,38,26,9,3,90,36,54)
    val ligueA20182 = LeagueStanding("LigueA",2018,2,"Atlético de Madrid",76,38,22,10,6,55,29,26)

    val leaguestandings = spark.createDataset(Seq(ligue20171,ligue20172,ligue20181,ligue20182, ligueA20171, ligueA20172, ligueA20181 ,ligueA20182))
    val re1 = MostTitleTeamSpark("Ligue 1",2,"Paris Saint-Germain")
    val re2 = MostTitleTeamSpark("LigueA",2,"FC Barcelone")
    val expected = Array(re1,re2)

    //When
    val result:Dataset[MostTitleTeamSpark] = StatProcess.getMostTitelTeam(leaguestandings)
    //println("Teste 3 - the expected",expected)
    //println("Teste 3 - the result",result.collectAsList())
    //Then
    assert(expected.sameElements(result.collect()))
  }

  "Get Moste Title Team With SQL" should "OK" in{
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
    val re1 = MostTitleTeamSpark("Ligue 1",2,"Paris Saint-Germain")
    val re2 = MostTitleTeamSpark("LigueA", 2,"FC Barcelone")
    val expected = Array(re1,re2)

    //When
    val result:Dataset[MostTitleTeamSpark] = StatProcess.getMostTitelTeamSQL(leaguestandings)
    println("Teste 3 - the expected",expected)
    println("Teste 3 - the result",result.show())
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
    val result:Dataset[AveragePointWinnerSpark] = StatProcess.getAveragePointWinner(leaguestandings)
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
    val result:Dataset[LeagueStanding] = StatProcess.getFirstSecondLs(leagueStandingDS)

    result.show()

    //Then

    assert(result.collect().sameElements(expected.collect()))
  }
  "ce test permet de grouper par league et season les leagueStanding et calcule la difference de points" should "OK" in {
    //Given
    val leagueStandingDS = getDataSetLeagueDS()
    val expected  = spark.createDataFrame(Seq(("Ligue 1",2017,13),("Liga",2018,11)))

    //When
    val result = StatProcess.groupByLs(leagueStandingDS)

    result.show()

    //Then

    assert(result.collect().sameElements(expected.collect()))
  }


  "calcule de difference entre des listes" should "OK" in {
    //Given
    val leagueStanding1 = LeagueStanding("Ligue 1",2017,1,"Paris Saint-Germain",93,38,29,6,3,108,29,79)
    val leagueStanding2 = LeagueStanding("Ligue 1",2017,2,"AS Monaco",80,38,24,8,6,85,45,40)
    val leagueStanding3 = LeagueStanding("Ligue 1",2017,3,"Olympique lyonnais",78,38,23,9,6,87,43,44)
    val leagueStandingLS = List(leagueStanding1,leagueStanding2,leagueStanding3)
    val expected = 15
    //When
    val result = StatProcess.getDelta(leagueStandingLS.map(_.points))
    println("Teste diff - the expected",expected)
    println("Teste diff - the result",result)
    //Then
    assert(expected==result)
  }
  "ce test permet de calculer le maximum de points pour chaque league" should "ok" in {
    //Given
    val leagueStandingDS = getDataSetLeagueDS()
    val leagMax1 = LeagueMaxPoints("Ligue 1", 93)
    val leagMax2 = LeagueMaxPoints("Liga", 93)
    val expected= spark.createDataset(Seq(leagMax1,leagMax2))

    //Then
    val result = StatProcess.getMaxPointLeague(leagueStandingDS)
    result.show()
    expected.show()
    //When
    assert(expected.collect().sameElements(result.collect()))

  }

  "ce test permet de faire jointure 2 Dataset (DS leagueStanding DS maxPoint de chaque ligue" should "ok" in {
    //Given
    val leagueStandingDS = getDataSetLeagueDS()
    val leagMax1 = LeagueMaxPoints("Ligue 1", 93)
    val leagMax2 = LeagueMaxPoints("Liga", 93)
    //val expected= spark.createDataset(Seq(leagMax1,leagMax2))
    val leagueStanding1 =("Ligue 1",2017,1,"Paris Saint-Germain",93,38,29,6,3,108,29,79.0,93)
    val leagueStanding2 = ("Ligue 1",2017,2,"AS Monaco",80,38,24,8,6,85,45,40.0,93)
    val leagueStanding3 = ("Ligue 1",2017,3,"Olympique lyonnais",78,38,23,9,6,87,43,44.0,93)

    val leagueStanding4 = ("Ligue 1",2018,1,"Paris Saint-Germain",91,38,29,4,5,105,35,70.0,93)
    val leagueStanding5 = ("Ligue 1",2018,2,"LOSC",75,38,22,9,7,68,33,35.0,93)
    val leagueStanding6 = ("Ligue 1",2018,3,"lympique lyonnais",72,38,21,9,8,70,47,23.0,93)

    val leagueStanding7 = ("Liga",2017,1,"FC Barcelone",93,38,28,9,1,99,29,70.0,93)
    val leagueStanding8 = ("Liga",2017,2,"Atlético de Madrid",79,38,23,10,5,58,22,36.0,93)
    val leagueStanding9 = ("Liga",2017,3,"Real Madrid",76,38,22,10,6,94,44,50.0,93)

    val leagueStanding10 = ("Liga",2018,1,"FC Barcelone",87,38,26,9,3,105,36,54.0,93)
    val leagueStanding11 = ("Liga",2018,2,"Atlético de Madrid",76,38,22,10,6,105,29,26.0,93)
    val leagueStanding12 = ("Liga",2018,3,"Raal Madrid",68,38,21,5,12,63,46,17.0,93)
    val expected = spark.createDataFrame(Seq(leagueStanding1,leagueStanding3,leagueStanding2,
      leagueStanding4,leagueStanding5,leagueStanding6,leagueStanding7,leagueStanding8,leagueStanding9,
      leagueStanding10,leagueStanding11,leagueStanding12))
    //Then
    val result = StatProcess.getLSJointMaxPoints(leagueStandingDS)
    result.show()
    expected.show()
    //When
    assert(result.collect().sameElements(expected.collect()))

  }

  "ce test permet de faire l   jointure entre  deux Dataset en utilisant window function Spark" should "ok" in {

    //Given
    val leagueStandingDS = getDataSetLeagueDS()
    val leagMax1 = LeagueMaxPoints("Ligue 1", 93)
    val leagMax2 = LeagueMaxPoints("Liga", 93)
    //val expected= spark.createDataset(Seq(leagMax1,leagMax2))
    val leagueStanding1 =("Ligue 1",2017,1,"Paris Saint-Germain",93,38,29,6,3,108,29,79.0,93)
    val leagueStanding2 = ("Ligue 1",2017,2,"AS Monaco",80,38,24,8,6,85,45,40.0,93)
    val leagueStanding3 = ("Ligue 1",2017,3,"Olympique lyonnais",78,38,23,9,6,87,43,44.0,93)

    val leagueStanding4 = ("Ligue 1",2018,1,"Paris Saint-Germain",91,38,29,4,5,105,35,70.0,93)
    val leagueStanding5 = ("Ligue 1",2018,2,"LOSC",75,38,22,9,7,68,33,35.0,93)
    val leagueStanding6 = ("Ligue 1",2018,3,"lympique lyonnais",72,38,21,9,8,70,47,23.0,93)

    val leagueStanding7 = ("Liga",2017,1,"FC Barcelone",93,38,28,9,1,99,29,70.0,93)
    val leagueStanding8 = ("Liga",2017,2,"Atlético de Madrid",79,38,23,10,5,58,22,36.0,93)
    val leagueStanding9 = ("Liga",2017,3,"Real Madrid",76,38,22,10,6,94,44,50.0,93)

    val leagueStanding10 = ("Liga",2018,1,"FC Barcelone",87,38,26,9,3,105,36,54.0,93)
    val leagueStanding11 = ("Liga",2018,2,"Atlético de Madrid",76,38,22,10,6,105,29,26.0,93)
    val leagueStanding12 = ("Liga",2018,3,"Raal Madrid",68,38,21,5,12,63,46,17.0,93)
    val expected = spark.createDataset(Seq(leagueStanding1,leagueStanding3,leagueStanding2,
      leagueStanding4,leagueStanding5,leagueStanding6,leagueStanding7,leagueStanding8,leagueStanding9,
      leagueStanding10,leagueStanding11,leagueStanding12))
    //When
    val result = StatProcess.getLSJoinedByMaxWindowFunction(leagueStandingDS)
    result.show()
    expected.show()
    //Then
    assert(result.collect().sameElements(expected.collect()))
  }



  "ce test permet de faire l   jointure entre  deux Dataset en utilisant window function Spark SQL" should "ok" in {

    //Given
   val leagueStandingDS = getDataSetLeagueDS()
    val leagMax1 = LeagueMaxPoints("Ligue 1", 93)
    val leagMax2 = LeagueMaxPoints("Liga", 93)
    //val expected= spark.createDataset(Seq(leagMax1,leagMax2))
    val leagueStanding1 =("Ligue 1",2017,1,"Paris Saint-Germain",93,38,29,6,3,108,29,79.0,93)
    val leagueStanding2 = ("Ligue 1",2017,2,"AS Monaco",80,38,24,8,6,85,45,40.0,93)
    val leagueStanding3 = ("Ligue 1",2017,3,"Olympique lyonnais",78,38,23,9,6,87,43,44.0,93)

    val leagueStanding4 = ("Ligue 1",2018,1,"Paris Saint-Germain",91,38,29,4,5,105,35,70.0,93)
    val leagueStanding5 = ("Ligue 1",2018,2,"LOSC",75,38,22,9,7,68,33,35.0,93)
    val leagueStanding6 = ("Ligue 1",2018,3,"lympique lyonnais",72,38,21,9,8,70,47,23.0,93)

    val leagueStanding7 = ("Liga",2017,1,"FC Barcelone",93,38,28,9,1,99,29,70.0,93)
    val leagueStanding8 = ("Liga",2017,2,"Atlético de Madrid",79,38,23,10,5,58,22,36.0,93)
    val leagueStanding9 = ("Liga",2017,3,"Real Madrid",76,38,22,10,6,94,44,50.0,93)

    val leagueStanding10 = ("Liga",2018,1,"FC Barcelone",87,38,26,9,3,105,36,54.0,93)
    val leagueStanding11 = ("Liga",2018,2,"Atlético de Madrid",76,38,22,10,6,105,29,26.0,93)
    val leagueStanding12 = ("Liga",2018,3,"Raal Madrid",68,38,21,5,12,63,46,17.0,93)
    val expected = spark.createDataset(Seq(leagueStanding1,leagueStanding3,leagueStanding2,
      leagueStanding4,leagueStanding5,leagueStanding6,leagueStanding7,leagueStanding8,leagueStanding9,
      leagueStanding10,leagueStanding11,leagueStanding12))
    //When
    val result = StatProcess.getLSJoinedByMaxWindowFunctionSQL(leagueStandingDS)
    result.show()
    expected.show()
    //Then
    assert(result.collect().sameElements(expected.collect()))
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
