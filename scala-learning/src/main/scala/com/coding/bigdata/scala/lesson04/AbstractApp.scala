package com.coding.bigdata.scala.lesson04

object AbstractApp {

  def main(args: Array[String]): Unit = {
    val student = new Student2()
    student.speak
  }
}

/**
 * 类的一个或者多个方法没有完整的实现（只有定义，没有实现）
 */
abstract class Person2 {
  def speak

  val name: String
  val age: Int
}

class Student2 extends Person2 {
  override def speak: Unit = {
    println("speak")
  }

  override val name: String = "lm"
  override val age: Int = 18
}
