package com.test.spark.wiki.extracts

import org.apache.spark.sql.Dataset
import ImpliciteBigapps._

object SeasonUtils {
  def leagtoSeason(league:League, debut:Int, fin:Int): Seq[Season] = {
    (debut to fin).toList.map(year => Season(league.name,league.url.format(year,year+1),year))

  }

  def convertoseason(leagueDs: Dataset[League], y1:Int, y2:Int): Dataset[Season] ={
    leagueDs.flatMap(l => leagtoSeason(l,y1,y2))

  }
}
