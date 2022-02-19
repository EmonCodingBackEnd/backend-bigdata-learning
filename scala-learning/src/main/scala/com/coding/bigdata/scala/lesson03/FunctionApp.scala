package com.coding.bigdata.scala.lesson03

object FunctionApp {

  /*
   * Scala函数：方法的定义和使用
   * def    开始函数定义
   * main   函数名
   * args   括号中的参数列表，逗哥参数之间英文逗号分隔
   * Unit   函数返回值类型
   * =      等号
   * 例如：
   * def 方法名(参数名:参数类型): 返回值类型 = {
   *    // 括号内的叫做方法体
   *    // 方法体内的最后一行为返回值，不需要使用return
   * }
   */
  def main(args: Array[String]): Unit = {
    // 大括号中的函数体
    /*println(add(2, 3))
    println(three())
    println(three) // 没有入参的函数，调用时括号是可以省略的*/

    /*sayHello()
    sayHello("lm")*/

    /*sayName()
    sayName("神奇的人")
    loadConf()
    loadConf("spark-production.conf")*/

    /*println(speed(100, 10))
    println(speed(distance = 100, time = 10))
    println(speed(time = 10, distance = 100))*/

    println(sum(1, 2))
    println(sum(1, 2, 3, 4, 5))
    println(sum(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
  }

  def add(x: Int, y: Int): Int = {
    x + y
  }

  def three() = 1 + 2

  def sayHello(): Unit = {
    println("Say Hello......")
  }

  def sayHello(name: String): Unit = {
    println("Say Hello......" + name)
  }

  // 默认参数：在函数定义时，允许指定参数的默认值
  def sayName(name: String = "lm"): Unit = {
    println(name)
  }

  def loadConf(conf: String = "spark-defaults.conf"): Unit = {
    println(conf)
  }

  // 命名参数：
  def speed(distance: Float, time: Float): Float = {
    distance / time
  }

  // 可变参数：
  def sum(number: Int*) = {
    var result = 0
    for (elem <- number) {
      result += elem
    }
    result
  }
}
