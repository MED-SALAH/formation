package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import org.jsoup.Jsoup
//import scala.collection.JavaConversions._


object StatUtils {
  def getAveragePointWinner(ligue: Seq[League])(implicit spark:SparkSession): Seq[AveragePointWinner] ={
    //val createdYear = getCreatedYear(ligue)
    val createdYear = 2014
    //val currentYear = Year.now.getValue
    val currentYear = 2018
    //val listSeason:Seq[Season] = SeasonUtils.leagtoSeason(,createdYear,currentYear)

    ligue.map(lig => {
      val listSeason:Seq[Season] = SeasonUtils.leagtoSeason(lig,createdYear,currentYear)
      AveragePointWinner(lig,listSeason.map(seas => {
        (SeasonScraper.scraper(seas).head.points.toDouble/listSeason.length)
      }).reduce((a,b)=>a+b))
    })


  }

  def getMostTitelTeam(ligue:Seq[League]): Seq[MostTitleTeam] ={
    //val createdYear = getCreatedYear(ligue)
    val createdYear = 2015
    //val currentYear = Year.now.getValue
    val currentYear = 2019

    //println(listSeason)
    ligue.map(lig => {
      val listSeason:Seq[Season] = SeasonUtils.leagtoSeason(lig,createdYear,currentYear)
      MostTitleTeam(lig,listSeason.map(l =>{
        SeasonScraper.scraper(l).head.team
      }).groupBy(identity).maxBy(_._2.size)._1)
    })

    //println(MostTitleTeam)


  }

  def getCreatedYear(ligue: League): Int = {

    val doc = Jsoup.connect(ligue.url.dropRight(6)).get()
    val info = doc.select("table").first()
    val yearleague = info.select("td").get(1).text().toInt
    return yearleague

  }

  def getAverageGoalsbySeason(season: Seq[Season]): Seq[AvrageGols] ={
    season.map(s => {
      val list = SeasonScraper.scraper(s)
      AvrageGols(s,list.map(l=> (l.goalsFor.toDouble/list.length)).reduce((a,b)=>a+b))
      })
  }

}
