package com.test.spark.wiki.extracts

class TestCarrefour {
  def tri(): Unit ={
    // .sortBy( elt => (elt._1,-elt._2))
  }


  def min(): Unit ={
    //ts.map( elt => (Math.abs(elt), elt) ).sortBy( elt => (elt._1,-elt._2)).head._2
  }

  def foldLeftR(): Unit ={
    //val ts = List(0,1,2,3)
    //val defaultSum =  0
    //ts.foldLeft(defaultSum)( (sum, elt) => sum+elt)
  }


  def flatMapEx(): Unit ={
    //ts.flatMap(elt=> {
    //       (1 to elt).map(r => 1)
    //     }).toList
  }


}


import math._
import scala.util._
import scala.io.StdIn._

object Solution extends App {

  def calculateTotalPrice(prices: Array[Int], discount: Int): Int = {
    // Write your code here
    // To debug: System.err.println("Debug messages...");
    (discount, prices.size) match {
      case (d, p) if (d >= 0 && d <= 100 && p > 0 && p < 100) => {
        val sorted = prices
          .filter(product => product > 0 && product < 10000)
          .sortBy(-_
          )
        val unDiscountedProducts = sorted.tail.sum
        val discountedProduct = Math.round(sorted.head*(100-discount)/100)
        unDiscountedProducts + discountedProduct
      }
      case _ => prices.sum
    }

  }

  def calculMultiple(n: Int): Int = {
    val list = List(3,5,7).toArray
    list.flatMap(elt => {(1 to n/elt)
      .map(e => { e * elt })
      .filter(elt => {(elt < n)})
    }).toSet.sum
  }

  val solution = calculMultiple(100)
  println(s"solution = ${solution}")
  //val  princes = List(100, 30, 500).toArray

  //println(solution)
  //val discount = 30
  //val solution = calculateTotalPrice(princes, discount)


}
