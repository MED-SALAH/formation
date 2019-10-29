package com.test.spark.wiki.extracts

import com.test.spark.wiki.extracts.ImpliciteBigapps._
import com.test.spark.wiki.extracts.ImpliciteBigapps.spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.jsoup.Jsoup


object StatUtilisSpark {
  def getDelta(leagueStandingLS: LeagueStanding): DataFrame[getTightenLS] ={
    val init = getTightenLS(0,leagueStandingLS.season)
    val cuurent =
    leagueStandingLS.foldLeft((a,b)=>(a.points - b.points)))
    val head = leagueStandingLS.head
    val tail = leagueStandingLS.tail

    tail.fold

    spark.createDataFrame(Seq())
  }

  def groupByLs(leagueStandingDS: Dataset[LeagueStanding]): DataFrame ={
    val test = getFirstSecondLs(leagueStandingDS)
      .groupBy("league","season")
      .agg(first(col("points"))
        .minus(last(col("points"))).as("Diff")).orderBy("season").show()
        //.groupBy("league","season").agg(min("Diff")).orderBy("Diff").first()
    spark.createDataFrame(Seq())

  }

  def getFirstSecondLs(leagueStandingDS: Dataset[LeagueStanding]): Dataset[LeagueStanding] ={
    leagueStandingDS.filter(elt => {
      elt.position==1||elt.position==2
    })

  }

  def getAveragePointWinner(liguest: Dataset[LeagueStanding]): Dataset[AveragePointWinnerSpark] ={
    liguest
      .repartition($"league")
      .filter(_.position == 1)
      .groupBy("league")
      .avg("points")
      .map(t => AveragePointWinnerSpark(t.getString(0),t.getDouble(1)))

  }


  def getMostTitelTeam(liguest: Dataset[LeagueStanding]): Dataset[MostTitleTeamSpark] ={

    //println(liguest.collect().toList)
    val teamMostTitls = liguest
      .repartition($"league")
      .filter(_.position == 1)
      .groupBy("league","team")
      .count()

    teamMostTitls.map(t=> MostTitleTeamSpark(t.getString(0),t.getString(1))).orderBy($"league")



  }



  def getCreatedYear(ligue: League): Int ={
    val doc = Jsoup.connect(ligue.url.dropRight(6)).get()
    val info = doc.select("table").first()
    val yearleague = info.select("td").get(1).text().toInt
    return yearleague
  }

  def getAverageGoalsbySeason(season: Dataset[Season]): Dataset[AvrageGols] ={
    season.map(s =>{
      val data = SeasonScraper.scraper(s)
      AvrageGols(s,data.map(l=> (l.goalsFor.toDouble/data.length)).reduce((a,b)=>a+b))
    })
  }

}
