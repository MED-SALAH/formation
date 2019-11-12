package com.test.spark.wiki.extracts.processors

import com.test.spark.wiki.extracts.domains.{ImpliciteBigapps, League}
import org.apache.spark.sql.{Dataset, SparkSession}

object LeagueProcess {
  import ImpliciteBigapps._
  implicit def convert(seq:Seq[League])(implicit spark: SparkSession): Dataset[League] ={
    spark.createDataset(seq)
  }

}
