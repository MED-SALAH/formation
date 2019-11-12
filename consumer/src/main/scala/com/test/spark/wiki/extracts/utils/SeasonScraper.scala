package com.test.spark.wiki.extracts.utils

import com.test.spark.wiki.extracts.domains.{LeagueStanding, Season}
import org.jsoup.Jsoup
import scala.collection.JavaConversions._
object SeasonScraper {
  def scraper(saison:Season): Seq[LeagueStanding] ={

    val doc = Jsoup.connect(saison.url).get()
    val table = doc.select("table[class = wikitable gauche]").first()
    val title = doc.title()
    val trs = table.select("tbody > tr").next().toList
    //val tet = table.select("tr").select("td").get(2).text()
    //println(tet)

    trs.map(tr => {
      val tds = tr.select("td")
      val test = tds
      test.select("span > a").text() match{
        case "" => LeagueStanding(saison.name,
          saison.year,
          tds.get(0).text.toInt,
          tds.select("span > b > a").get(0).text,
          tds.get(2).text.toInt,
          tds.get(3).text.toInt,
          tds.get(4).text.toInt,
          tds.get(5).text.toInt,
          tds.get(6).text.toInt,
          tds.get(7).text.toInt,
          tds.get(8).text.toInt,
          tds.get(9).text().replaceAll(",",".").toDouble)
        case _ => LeagueStanding(saison.name,
          saison.year,
          tds.get(0).text.toInt,
          tds.select("span > a").get(0).text,
          tds.get(2).text.toInt,
          tds.get(3).text.toInt,
          tds.get(4).text.toInt,
          tds.get(5).text.toInt,
          tds.get(6).text.toInt,
          tds.get(7).text.toInt,
          tds.get(8).text.toInt,
          tds.get(9).text().replaceAll(",",".").toDouble)
      }

    })


  }

}
