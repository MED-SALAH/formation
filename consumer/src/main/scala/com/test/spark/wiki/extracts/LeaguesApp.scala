package com.test.spark.wiki.extracts

import com.datastax.spark.connector._
import com.test.spark.wiki.extracts.domains.ImpliciteBigapps._
import com.test.spark.wiki.extracts.processors.{LeagueProcess, SeasonProcess, StatProcess}
import com.test.spark.wiki.extracts.utils.{Parser, SeasonScraper}
import org.apache.spark.sql.SparkSession

object  LeaguesApp {


  def main(args: Array[String]): Unit = {
    implicit  val  spark: SparkSession=SparkSession
      .builder()
      .master("local[*]")
      .config("spark.cassandra.connection.host", "35.180.46.40")
      .getOrCreate()

    /*    val conf = new SparkConf()
        conf.setMaster("local")
        conf.setAppName("Word Count")
        val sc = new SparkContext(conf)
    */
    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    //val textFile = sc.textFile("C:/spark/spark-2.4.4-bin-hadoop2.7/README.md")

    //word count
    /*val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println)
    System.out.println("Total words: " + counts.count());
    counts.saveAsTextFile("/tmp/shakespeareWordCount")*/

    //
    val leagueFilePath = "src/test/resources/leagues.yaml"
    val doc = Parser.parse(leagueFilePath)
    println(doc)
    val ligDs = LeagueProcess.convert(doc)

    val seasonDs = SeasonProcess.convertoseason(ligDs,2014,2018)
    val ligStanding = seasonDs.flatMap(s => SeasonScraper.scraper(s))
    val avgGolsSeason = StatProcess.getAverageGoalsbySeason(seasonDs)
    avgGolsSeason.show()
    println("Moste titled team ===>")
    val mostTitleTeam = StatProcess.getMostTitelTeam(ligStanding)
    mostTitleTeam.show()
    mostTitleTeam.rdd.saveToCassandra("formation", "mosttitleteam", SomeColumns("league", "nbrwins", "team"))



    //
    //    val avgPointWiner = StatProcess.getAveragePointWinner(ligStanding)
    //    avgPointWiner.show()
    //    val groupBys = StatProcess.groupByLs(ligStanding)
    //    groupBys.show()
    //    val maxPointLeagues = StatProcess.getMaxPointLeague(ligStanding)
    //    maxPointLeagues.show()
    //    val maxPointslsJoin = StatProcess.getLSJointMaxPoints(ligStanding)
    //    maxPointslsJoin.show()
    //    maxPointslsJoin.repartition(1).write
    //      .format("com.databricks.spark.csv")
    //      .option("header", "true")
    //      .save("maxPointLsJoin.csv")

  }


}