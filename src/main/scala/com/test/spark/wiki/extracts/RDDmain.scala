package com.test.spark.wiki.extracts

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import RDDexample._


object RDDmain {
  def main(args: Array[String]): Unit = {

    val header = "time duration client_add result_code bytes req_method url user hierarchy_code type"
    val filep = "C:\\www\\formation2salah\\formation\\src\\main\\resources\\textRDD"
    println("le Header : "+header +"\n"+"le File Path :"+filep)
    RDDexample.readTextFile(filep,header)

  }

}
