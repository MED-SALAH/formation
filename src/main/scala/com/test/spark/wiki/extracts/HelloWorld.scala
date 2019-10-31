package com.test.spark.wiki.extracts
import com.test.spark.wiki.extracts.ImpliciteBigapps._
import org.apache.spark.sql.SparkSession

object  HelloWorld {


  def main(args: Array[String]): Unit = {
    implicit  val  spark: SparkSession=SparkSession
      .builder()
      .master("local[*]")
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
    val leagueFilePath = "leagues.yaml"
    val doc = Parser.parse(leagueFilePath)
    println(doc)
    val ligDs = LeagueUtils.convert(doc)
    ligDs.toDF().show()
    val seasonDs = SeasonUtils.convertoseason(ligDs,2014,2018)
    seasonDs.show()

    val ligStanding = seasonDs.flatMap(s => SeasonScraper.scraper(s))
    ligStanding.toDF().show()
    val avgGolsSeason = StatUtilisSpark.getAverageGoalsbySeason(seasonDs)
    avgGolsSeason.show()
    println("Moste titled team ===>")
    val mostTitleTeam = StatUtilisSpark.getMostTitelTeam(ligStanding)
    mostTitleTeam.show()
    val avgPointWiner = StatUtilisSpark.getAveragePointWinner(ligStanding)
    avgPointWiner.show()
    val groupBys = StatUtilisSpark.groupByLs(ligStanding)
    groupBys.show()
    val maxPointLeagues = StatUtilisSpark.getMaxPointLeague(ligStanding)
    maxPointLeagues.show()
    val maxPointslsJoin = StatUtilisSpark.getLSJointMaxPoints(ligStanding)
    maxPointslsJoin.show()
    maxPointslsJoin.repartition(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("maxPointLsJoin.csv")



  }


}