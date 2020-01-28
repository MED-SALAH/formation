package com.test.spark.wiki.extracts


abstract class Voiture(name:String, vitess:Int){

}

class Renault(name:String, vitess:Int, couleur:String) extends Voiture(name, vitess) with Photo {
  def getname()={
    println(name)
  }
  def getvitess()={
    println(vitess)
  }

  //println(couleur)
}

trait Photo{
  def calcul(): Unit ={
    println("pixel")
  }
}

object TestSalah2 {
  def main(args: Array[String]): Unit = {
    val renault = new Renault("clio",233,"gris")
    renault.getname()
    renault.getvitess()

  }

}
