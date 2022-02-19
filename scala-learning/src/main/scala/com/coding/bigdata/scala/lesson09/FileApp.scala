package com.coding.bigdata.scala.lesson09

import scala.io.Source

object FileApp extends App {

  val path: String = "custom/data/scala/a.txt"
  val file = Source.fromFile(path)(scala.io.Codec.UTF8)

  /*def readLine(): Unit = {
    for (line <- file.getLines()) {
      println(line)
    }
  }

  readLine()*/

  /*def readChar(): Unit = {
    for (e <- file) {
      println(e)
    }
  }

  readChar()*/

  def readNet(): Unit = {
    val file = Source.fromURL("http://www.baidu.com")
    for (line <- file.getLines()) {
      println(line)
    }
  }

  readNet()
}
