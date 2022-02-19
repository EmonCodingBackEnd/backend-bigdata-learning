package com.coding.bigdata.scala.lesson03

import scala.io.Source

object LazyApp extends App {

  lazy val lines = Source.fromFile("custom/data/scala/a.txt")
  println(lines.mkString)
}
