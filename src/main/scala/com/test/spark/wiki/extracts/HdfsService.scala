package com.test.spark.wiki.extracts

import java.io.{File, FileNotFoundException}
import java.nio.file.{CopyOption, Files, Path, Paths, StandardCopyOption}

import org.apache.hadoop.fs.Options.CreateOpts.BlockSize

import scala.io.Source

object HdfsService {
  def save(data: Seq[String], filePath: String): Any = {


  }


  def readLines(filePath: String, from: Int, blocSize: Int): List[String] = {
    val lines = Source.fromFile(filePath).getLines
    val selectedLines = lines drop(from)

    selectedLines.zipWithIndex.filter{
      case (line, index) => {
        println(s" Index ${index} and  (from+linesNumber) ${  (blocSize) }")
        index < (blocSize)
      }
    }.map(_._1).toList

  }

  def split(filePath: String, destPath:String, blocsNumber: Int, blockSize:Int)(implicit ROOT_PATH:String) = {
    Array.range(0, blocsNumber).foreach(index =>{
      val from = (index * blockSize)
      val lines:List[String] = readLines(filePath, from = from, blockSize)

      //save lines into file
      val exexutor = new BigAppsHdfsExecutor(s"ex_${index}")

      //linesSrcPath =
      //exexutor.put(linesSrcPath, destPath)

    })

  }




  //returns line number of a file
  def getLineNumber(fileName: String): Integer = {
    val src = Source.fromFile(fileName)
    try {
      src.getLines.size
    } catch {
      case error: FileNotFoundException => -1
      case error: Exception => -1
    }
    finally {
      src.close()
    }
  }

  def put(srcAbsolutePath: String, destPath: String)(implicit ROOT_PATH:String) = {

    val srcFile = new File(srcAbsolutePath)
    val destAbsolutePath = s"${ROOT_PATH}/${destPath}"
    val destFolder = new File(destAbsolutePath)

    if(!destFolder.exists()) destFolder.mkdirs()

    val destFile = new File(destAbsolutePath+"/"+srcFile.toPath.getFileName)

    Files.copy(
      srcFile.toPath,
      destFile.toPath,
      StandardCopyOption.REPLACE_EXISTING
    )


  }

}
