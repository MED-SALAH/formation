package com.test.spark.wiki.extracts

object BIgAppsHdfsNameNode {

  def split(srcPath: String, destPath:String,  execNumber: Int)(implicit ROOT_PATH:String) = {

    val nbLines = HdfsService.getLineNumber(srcPath)

    val blocSize = 2

    val nbBlocs = (nbLines % execNumber == 0) match {
      case true => nbLines / execNumber
      case false => (nbLines / execNumber) + 1
    }

    HdfsService.split(srcPath, destPath, nbBlocs, blockSize = blocSize)
  }

}
