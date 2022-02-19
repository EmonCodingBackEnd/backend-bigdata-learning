package com.coding.bigdata.scala.lesson06

import java.io.{FileNotFoundException, IOException}

object ExceptionApp extends App {

  val i = 10 / 2
  println(i)

  try {
    val j = 10 / 0
    println(j)
  } catch {
    case e: ArithmeticException => println("除数不能为0")
    case e: Exception => println(e.getMessage)
  } finally {
    // 释放资源，一定能执行
  }

  val file = "test.txt"
  try {
    // open file
    // use file
    val lines = scala.io.Source.fromFile(file).mkString
    println(file)
  }
  catch {
    case e: FileNotFoundException => println("no file")
    case e: IOException => println("io exception")
    case e: Exception => println("exception")
  }
  finally {
    // 释放资源，一定能执行：close file
  }

}
