package com.test.spark.wiki.extracts.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.test.spark.wiki.extracts.domains.League

object Parser {

  def parse(path:String): Seq[League] ={
    path match {
      case  "" =>
        Seq.empty
      case _  =>
        val mapper = new ObjectMapper(new YAMLFactory())
        mapper.registerModule(DefaultScalaModule)
        val inputStream = getClass().getClassLoader().getResourceAsStream(path)
        mapper.readValue(inputStream, classOf[Array[League]]).toSeq
    }

  }

}
