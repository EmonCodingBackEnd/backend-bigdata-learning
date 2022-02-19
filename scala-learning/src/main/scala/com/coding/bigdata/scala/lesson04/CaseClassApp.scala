package com.coding.bigdata.scala.lesson04

// 通常用在模式匹配
object CaseClassApp {
  def main(args: Array[String]): Unit = {
    println(Dog("wangcai").name)
  }
}

// case class不用new

/*
 * case class：称为样例类，类似于Java中的JavaBean，只定义field，Scala自动提供getter和setter方法，没有method。
 * case class的主构造函数接收的参数通常不需要使用var或val修饰，Scala会自动使用val修饰。
 * Scala自动为case class定义了伴生对象，也就是object。
 * 并且定义了apply()方法，该方法接收主构造函数中相同的参数，并返回case class对象。
 * @param name
 */
case class Dog(name: String)