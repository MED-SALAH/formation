package com.test.spark.wiki.extracts

import java.io.{BufferedWriter, File, FileInputStream, FileWriter}
import org.apache.spark.sql.functions._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{Dataset, SparkSession}
import org.json4s.FileInput
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import Implicit._
import Ingestion._
import java.io._

object TestIngestion {
  def main(args: Array[String]): Unit = {

    //Ingestion.readSql("C:\\www\\formation2salah\\formation\\src\\main\\resources\\ingestion.sql")

    val result:Seq[Flux] = Ingestion.readfile("test.yaml")
    result.map(p => {
      p.name match {
        case "fichierCsv" =>
          val myDS = Ingestion.readcsvDS(p.filepath)
          myDS.show(50)
          myDS.createOrReplaceTempView("DFtable")
//          Ingestion.readSql("C:\\www\\formation2salah\\formation\\src\\main\\resources\\ingestion.sql")
        case "fichierSql" =>
          println(p.filepath)
          val res = Ingestion.readSql(p.filepath)
          println(res)

          //res.filter(col("Nbr")===1).show()
          //println(res.filter(col("Nbr")===1).count())

        case "alert" =>
          val alert:Seq[Alert] = Ingestion.readalert("alert.yaml")
          val res = Ingestion.readSql(p.filepath)
          val cnt = res.filter(col("Nbr")===1).count()


          alert.map(a => {
            a.namealert match {
              case "vente insufissante" =>
                println(a.expression)
                val file = new File("C:\\www\\formation2salah\\formation\\src\\main\\resources\\alerttt.txt" )
                val bw = new BufferedWriter(new FileWriter(file))
                bw.write(a.namealert)
                bw.close()

              case "achat insufissante" =>
                println(a.expression)
                println(a.action)
            }

          })

      }

    })



  }

}
