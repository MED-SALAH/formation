package com.test.spark.wiki.extracts

import java.io.File
import java.nio.file.Files

import scala.reflect.io.Directory

object HdfsUtilsTest {
  def delete(filePath: String)(implicit ROOT_PATH:String): Any = {
    val file = new File(ROOT_PATH+filePath)

    if (file.isFile){
      file.delete()
    }else{
      new Directory(file).deleteRecursively()
    }

  }

  def exists(filePath: String)(implicit ROOT_PATH:String): Boolean = {

    println(s"filePath to test = ${filePath} on the root ${ROOT_PATH}")
    val path =  new File(ROOT_PATH+filePath).getPath


    (new java.io.File(path).exists)
  }

}
