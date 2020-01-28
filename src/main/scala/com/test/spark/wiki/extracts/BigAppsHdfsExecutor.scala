package com.test.spark.wiki.extracts

class BigAppsHdfsExecutor(name:String)(implicit ROOT_PATH:String) {
  def put(src:String, dest:String): Unit ={
    HdfsService.put(src, s"/${name}/${dest}")
  }
}
