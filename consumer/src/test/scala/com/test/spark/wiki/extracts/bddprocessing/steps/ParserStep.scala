package com.test.spark.wiki.extracts.bddprocessing.steps

import com.test.spark.wiki.extracts.utils.Parser
import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}
import javafx.scene.chart.PieChart.Data
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.Matchers

class ParserStep  extends EN with ScalaDsl with Matchers {

  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()


  var df: DataFrame = _
  Given("""^la table suivante :$""") { (table : DataTable) =>
    df = Parser.parseDataTable(table)
  }

  When("""^lors ce que j'appelle le parser$""") { () =>

  }

  Then("""^j'aurai le name suivant :$""") { (table: DataTable) =>
    val name = table.raw().get(0).get(0)
    val nameDf = df.collectAsList().get(0).get(0)

    assert(name == nameDf)
  }

  And("""^j'aurai l'age suivant :$""") { (table: DataTable) =>
    val age = table.raw().get(0).get(0)
    val ageDF = df.collectAsList().get(0).get(1)

    assert(age.toString == ageDF.toString)
  }


  When("""^lors ce que j'appelle le parser :$""") { () =>

  }

  Then("""^j'aurai le schema de name suivant :$""") { (table: DataTable) =>

  }

  And("""^j'aurai le schema de age suivant :$""") { (table: DataTable) =>

  }



}
