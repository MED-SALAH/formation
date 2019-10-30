package com.test.spark.wiki.extracts

import com.test.spark.wiki.extracts.ImpliciteBigapps._
import com.test.spark.wiki.extracts.ImpliciteBigapps.spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.jsoup.Jsoup

import scala.collection.JavaConversions._


object StatUtilisSpark {
  def getMaxPointLeague(leagueStandingDS: Dataset[LeagueStanding]): Dataset[LeagueMaxPoints]= {
    leagueStandingDS.repartition($"league")
      .filter(_.position==1)
      .groupBy("league")
      .agg(max("points")).map(l => LeagueMaxPoints(l.getString(0),l.getInt(1)))
  }

  def getDelta(leagueStandingPoints: List[Int]): Int ={

    val head = leagueStandingPoints.head
    val tail = leagueStandingPoints.tail
    val init:(Int,Int) = (0,head)
    tail.foldLeft(init)((previousDeltat:(Int,Int),currentPoint:Int)=>{
      val prevDeltat:Int = previousDeltat._1
      val prevPoint: Int = previousDeltat._2
      (prevDeltat + (prevPoint-currentPoint), currentPoint)
    })._1

  }

  def groupByLs(leagueStandingDS: Dataset[LeagueStanding]): DataFrame ={
    getFirstSecondLs(leagueStandingDS)
      .groupBy("league","season")
      .agg(collect_list($"points"))
      .map(x => LeagueSeasonPoints(x.getString(0),x.getInt(1),getDelta(x.getList(2).toList)))
      .sort("league","listPoints").groupBy("league")
        .agg(
          first("season").as("season"),
          first("listPoints").as("listPoints")
        )


    //spark.createDataFrame(Seq())

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
