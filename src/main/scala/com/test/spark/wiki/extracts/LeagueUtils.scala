package com.test.spark.wiki.extracts

import org.apache.spark.sql.{Dataset, SparkSession}

object LeagueUtils {
  import ImpliciteBigapps._
  implicit def convert(seq:Seq[League])(implicit spark: SparkSession): Dataset[League] ={
    spark.createDataset(seq)

  }

}
