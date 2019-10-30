package com.test.spark.wiki.extracts

import org.apache.spark.sql.{Encoders, SparkSession}

object ImpliciteBigapps {
  implicit val encd = Encoders.product[League]
  implicit val encd2 = Encoders.product[Season]
  implicit val encd3 = Encoders.product[AvrageGols]
  implicit val encd4 = Encoders.product[MostTitleTeam]
  implicit val encd7 = Encoders.product[MostTitleTeamSpark]
  implicit val encd5 = Encoders.product[AveragePointWinner]
  implicit val encd6 = Encoders.product[LeagueStanding]
  //implicit val encd7 = Encoders.product[LeagueSeasonPoints]




  implicit  val  spark: SparkSession=SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()


}
