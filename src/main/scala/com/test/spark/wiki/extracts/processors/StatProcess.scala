package com.test.spark.wiki.extracts.processors

import com.test.spark.wiki.extracts.domains
import com.test.spark.wiki.extracts.domains._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, first, max}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.jsoup.Jsoup
import com.test.spark.wiki.extracts.domains.ImpliciteBigapps._
import com.test.spark.wiki.extracts.utils.SeasonScraper
import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._

object StatProcess {
  def getLSJoinedByMaxWindowFunctionSQL(leagueStandingDS: Dataset[LeagueStanding])(implicit spark:SparkSession): Dataset[(String,Int,Int,String,Int,Int,Int,Int,Int,Int,Int,Double,Int)] = {
    leagueStandingDS.createOrReplaceTempView("leagueStandigsTable")
    spark.sql(
      """
        |select *, max(points) OVER(PARTITION BY league) as maxPoints
        |from leagueStandigsTable
        |
        |""".stripMargin
    ).as[(String,Int,Int,String,Int,Int,Int,Int,Int,Int,Int,Double,Int)]

  }

  def getLSJoinedByMaxWindowFunction(leagueStandingDS: Dataset[LeagueStanding])(implicit spark:SparkSession):Dataset[(String,Int,Int,String,Int,Int,Int,Int,Int,Int,Int,Double,Int)] ={
    leagueStandingDS.repartition(col("league"))
      .withColumn("maxPoints",max(col("points"))
      .over(Window.partitionBy("league"))).as[(String,Int,Int,String,Int,Int,Int,Int,Int,Int,Int,Double,Int)]
  }

  def getMostTitelTeamSQL(leagueStandingsDS: Dataset[LeagueStanding])(implicit spark:SparkSession): Dataset[MostTitleTeamSpark] ={
    leagueStandingsDS.createOrReplaceTempView("leagueStandigsTable")
    spark.sql(
      """
        |select league, team, count(*) as count
        |from leagueStandigsTable
        |where position == "1"
        |group by league, team
        |order by league
        |
        |""".stripMargin).as[MostTitleTeamSpark]

  }


  def getLSJointMaxPoints(leagueStandingDS: Dataset[LeagueStanding])(implicit spark:SparkSession):DataFrame ={
    val maxLsDS = getMaxPointLeague(leagueStandingDS)
    leagueStandingDS.join(maxLsDS, "league")
      //leagueStandingDS.col("league").equalTo(maxLsDS.col("league")),
      //joinType = "left")
  }

  def getMaxPointLeague(leagueStandingDS: Dataset[LeagueStanding])(implicit spark:SparkSession): Dataset[LeagueMaxPoints]= {
    import spark.implicits._
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

  def groupByLs(leagueStandingDS: Dataset[LeagueStanding])(implicit spark:SparkSession): DataFrame ={
    import spark.implicits._
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

  def getAveragePointWinner(liguest: Dataset[LeagueStanding])(implicit spark:SparkSession): Dataset[AveragePointWinnerSpark] ={
    import spark.implicits._
    liguest
      .repartition($"league")
      .filter(_.position == 1)
      .groupBy("league")
      .avg("points")
      .map(t => AveragePointWinnerSpark(t.getString(0),t.getDouble(1)))

  }


  def getMostTitelTeam(liguest: Dataset[LeagueStanding])(implicit spark:SparkSession): Dataset[MostTitleTeamSpark] ={

    import spark.implicits._


    liguest.show()

    val teamMostTitls = liguest
      .repartition($"league")
      .filter(_.position == 1)
      .groupBy("league", "team")
      .count()
      .orderBy(asc("league"), desc("count"))

    val df =   teamMostTitls

    df.groupBy("league")
      .agg(
        first("team").as("team"),
        first("count").as("nbrwins"))

    teamMostTitls.show()

    teamMostTitls.map(t=> MostTitleTeamSpark(t.getString(0),t.getLong(2).toInt,t.getString(1))).orderBy($"league")



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
      domains.AvrageGols(s,data.map(l=> (l.goalsFor.toDouble/data.length)).reduce((a, b)=>a+b))
    })
  }

}
