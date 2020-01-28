package com.test.spark.wiki.extracts

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import Implicit._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions._
object Testsalah {
  implicit val saprk: SparkSession=SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
 // implicit val ecd = Encoders.product[Faits]
  def main(args: Array[String]): Unit = {
    def readcsvDS(file:String)(implicit spark:SparkSession):Dataset[Faits] = {
      spark.read
        .option("header","true")
        .option("inferSchema","true")
        .csv(file).as[Faits]
    }
    def readcsvDF(file:String)(implicit spark:SparkSession):DataFrame={
      spark.read
        .option("header","true")
        .option("inferSchema","true").option("delimiter",";")
        .csv(file)
    }
    def readParquet(file:String)(implicit spark:SparkSession):DataFrame={
      spark.read
        .option("header","true")
        .option("inferSchema","true").option("delimiter",";")
        .parquet(file)
    }
    println("hello Ds")
    val myDS = readcsvDS(file = "C:\\www\\formation2salah\\formation\\src\\main\\resources\\top50.csv")
    myDS.show()
    myDS.printSchema()
    //myDS.filter(_.Id=="1").show()
    val artist = myDS.repartition(col("Artist")).groupBy("Artist").count()
    artist.show()
    val energi = myDS.repartition(col("Track")).groupBy("Track","Energy").max("Energy")
      //.map(t=> Ener(t.getString(0),t.getInt(1)))
    energi.sort(desc("Energy")).limit(5).show()
    energi.map(t=>Ener(t.getString(0),t.getInt(1))).withColumn("exemple",col("Energy")*10).show()
    //energi.reduce(t=>Ener)
    println("----------------------------------------")

    println("hello DF")
    val myDF = readcsvDF(file = "C:\\www\\formation2salah\\formation\\src\\main\\resources\\bfaitc.csv")
    myDF.show()
    myDF.write.parquet("C:\\www\\formation2salah\\formation\\src\\main\\resources")
    val myDfParquet = readParquet(file = "myDF-Parquet")
    println("iciiiiii")
    myDfParquet.show()
    println("iciiiiii")
    //myDF.repartition(col("Langage")).filter(col("Langage") equals("C#")

   myDF.createOrReplaceTempView("DFtable")
    saprk.sql (
      """
        |select Langage, count(*)
        |from DFtable
        |group by Langage
        |""".stripMargin
    ).show()

  }

}
