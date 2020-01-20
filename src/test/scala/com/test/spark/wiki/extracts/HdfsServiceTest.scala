package com.test.spark.wiki.extracts

import java.io.File

import com.test.spark.wiki.extracts.HdfsService.getClass
import org.scalatest.{FlatSpec, FunSuite}

class HdfsServiceTest extends FlatSpec {

  "put file to Hdfs repo" should "OK" in{
    //Given
    implicit val ROOT_PATH:String = (new File(getClass.getClassLoader.getResource("./").getPath)).getAbsolutePath
    val srcPath:String = ROOT_PATH+"/local.txt"


    val destPath:String = "/bigapps/data/"
    val expectedfile:String = "/bigapps/data/local.txt"


    val deleteFolder:String = s"/bigapps"

    //When
    HdfsService.put(srcPath, destPath)

    //Then
    assert(HdfsUtilsTest.exists(expectedfile))

    HdfsUtilsTest.delete(deleteFolder)


  }



  "read file from line number" should "OK" in{
    //Given
    implicit val ROOT_PATH:String = (new File(getClass.getClassLoader.getResource("./").getPath)).getAbsolutePath

    val filePath = ROOT_PATH+"/local.txt"

    //When
    val lines:List[String] = HdfsService.readLines(filePath, 0, 2)

    //Then
    println(s"lines => ${lines}")
    assert(lines.size == 2)

  }

  "split file to multiple blocs" should "OK" in{
    //Given
    implicit val ROOT_PATH:String = (new File(getClass.getClassLoader.getResource("./").getPath)).getAbsolutePath

    val filePath = ROOT_PATH+"/local.txt"
    val blocsNumber = 5
    val blocPath1 = ROOT_PATH+"/1/local.txt"
    val blocPath2 = ROOT_PATH+"/2/local.txt"
    val blocPath3 = ROOT_PATH+"/3/local.txt"
    val blocPath4 = ROOT_PATH+"/4/local.txt"
    val blocPath5 = ROOT_PATH+"/5/local.txt"
    //When
    HdfsService.split(filePath, "/", blocsNumber, 2)

    //Then
    assert(HdfsUtilsTest.exists(blocPath1))
    assert(HdfsUtilsTest.exists(blocPath2))
    assert(HdfsUtilsTest.exists(blocPath3))
    assert(HdfsUtilsTest.exists(blocPath4))
    assert(HdfsUtilsTest.exists(blocPath5))

  }


  "save lines into file " should "OK" in{
    //Given
    //Given
    implicit val ROOT_PATH:String = (new File(getClass.getClassLoader.getResource("./").getPath)).getAbsolutePath

    val filePath = ROOT_PATH+"/save.txt"

    val data = Seq("A", "B")


    //When
    HdfsService.save(data, filePath)

    //Then
  }


}
