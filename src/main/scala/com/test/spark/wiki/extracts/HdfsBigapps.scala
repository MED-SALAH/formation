package com.test.spark.wiki.extracts

object HdfsBigapps {
  def main(args: Array[String]): Unit = {
    implicit val ROOT_PATH:String = "C:\\www\\formation2salah\\formation\\data\\hdfs\\data"

    val srcPath = s"C:\\www\\formation2salah\\formation\\data\\input\\mydata.txt"
    val destPath = "/output/"

    BIgAppsHdfsNameNode.split(srcPath, destPath, 3)


  }

}
