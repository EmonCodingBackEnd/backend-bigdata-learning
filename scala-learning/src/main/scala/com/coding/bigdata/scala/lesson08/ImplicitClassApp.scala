package com.coding.bigdata.scala.lesson08

object ImplicitClassApp extends App {

  implicit class Calculator(x: Int) {
    def add(a: Int) = a + x
  }

  println(1.add(3))
}



