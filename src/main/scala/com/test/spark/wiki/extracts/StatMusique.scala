package com.test.spark.wiki.extracts

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import com.test.spark.wiki.extracts.Implicit._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{lag, lead}
import org.apache.spark.sql.expressions.Window

object StatMusique {
  def getDeltaBeats(listFaits: Dataset[Faits]): DataFrame ={
    listFaits
      .repartition(col("BeatsPerMinute"))
      .withColumn("Lag", lag("BeatsPerMinute",1,0)
        .over(Window.orderBy("BeatsPerMinute")))
      .withColumn("Delta",col("BeatsPerMinute")-col("Lag"))
      //.groupBy("BeatsPerMinute")



  }

  def getTopByCol(listFaits: Dataset[Faits], N:Int,colName:String)(implicit spark:SparkSession): Dataset[Faits] ={
    listFaits
      .repartition(col("Id"))
      //.groupBy("Id","Track","Energy")
      //.count()
      //.map(t => Energy(t.getString(0),t.getString(1),t.getInt(2)))
      .sort(desc(colName))
      .limit(N)
  }

  def getFaitArtistCount(listfaits: Dataset[Faits])(implicit spark:SparkSession): Dataset[FaitArtist] = {
      listfaits
        .repartition(col("Artist"))
        .groupBy("Artist")
        .count()
        .map(t => FaitArtist(t.getString(0),t.getLong(1)))

  }


}
