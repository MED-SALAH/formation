package com.test.spark.wiki.extracts.processors

import com.test.spark.wiki.extracts.domains.{League, Season}
import org.apache.spark.sql.Dataset
import com.test.spark.wiki.extracts.domains.ImpliciteBigapps._

object SeasonProcess {
  def leagtoSeason(league:League, debut:Int, fin:Int): Seq[Season] = {
    (debut to fin).toList.map(year => Season(league.name,league.url.format(year,year+1),year))

  }

  def convertoseason(leagueDs: Dataset[League], y1:Int, y2:Int): Dataset[Season] ={
    leagueDs.flatMap(l => leagtoSeason(l,y1,y2))

  }
}
