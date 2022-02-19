package com.coding.bigdata.scala.lesson07

/**
 * 匿名函数：函数是可以命名的，也可以不命名
 * (参数名:参数类型...)=>参数体
 */
object FunctionApp extends App {
  /*def sayHello(name: String): Unit = {
    println("Hi: " + name)
  }

  sayHello("lm")

  // 把函数赋值给变量时，函数名后面要跟上 空格 _
  val sayaHelloFunc = sayHello _
  sayaHelloFunc("lm")*/


  /*// 匿名函数传递给变量
  val m1 = (x: Int) => x + 1
  println(m1(10))

  // 匿名函数传递给函数
  def add = (x: Int, y: Int) => x + y

  println(add(2, 3))*/


  /*// 将原来接收2个参数的一个函数，转换2个接收一个参数的函数，即是颗粒化函数：currying函数
  def sum(a: Int, b: Int) = a + b

  println(sum(2, 3))

  def sum2(a: Int)(b: Int) = a + b

  println(sum2(2)(3))*/


  /*val sayaHelloFunc = (name: String) => {
    println("hello," + name)
  }

  // 高级函数：函数作为参数
  def greeting(func: (String) => Unit, name: String): Unit = {
    func(name)
  }

  greeting(sayaHelloFunc, "lm")
  greeting((name: String) => println("hello," + name), "wh")
  greeting(name => println("hello," + name), "ls")*/


  // 常用高阶函数
  /*
  map
  flatMap
  foreach
  filter
  reduceLeft
   */

  val l = List(1, 2, 3, 4, 5, 6, 7, 8)
  /*// map:逐个去操作集合中的每个元素
  println(l.map((x: Int) => x + 1)) // // List(2, 3, 4, 5, 6, 7, 8, 9)
  println(l.map(x => x * 2)) // List(2, 4, 6, 8, 10, 12, 14, 16)
  // x => x * 2 简写为 _ * 2
  println(l.map(_ * 2)) // List(2, 4, 6, 8, 10, 12, 14, 16)*/


  /*println(l.map(_ * 2).filter(e => e > 8)) // List(10, 12, 14, 16)
  println(l.map(_ * 2).filter(_ > 8)) // List(10, 12, 14, 16)

  println(l.take(4)) // List(1, 2, 3, 4)*/


  /*// 1+2 3+3 6+4 10+5 15+6 21+7 28+8 => 36
  println(l.reduce(_ + _))
  // 1-2 -1-3 -4-4 -8-5 -13-6 -19-7 -26-8 => -34
  println(l.reduce(_ - _))
  // 1-2 -1-3 -4-4 -8-5 -13-6 -19-7 -26-8 => -34
  println(l.reduceLeft(_ - _))
  // 7-8 6-(-1) 5-7 4-(-2) 3-6 2-(-3) 1-5 => -4
  println(l.reduceRight(_ - _))*/


  /*// 初始种子0，累减操作：0-1 -1-2 -3-3 -6-4 -10-5 -15-6 -21-7 -28-8 => -36
  println(l.fold(0)(_ - _))
  println(l.foldLeft(0)(_ - _))
  // 初始种子0，累减操作，右边开始：8-0 7-8 6-(-1) 5-7 4-(-2) 3-6 2-(-3) 1-5 => -4
  println(l.foldRight(0)(_ - _))*/


  /*println(l.max)
  println(l.min)
  println(l.sum)*/


  /*val f = List(List(1, 2), List(3, 4), List(5, 6))
  println(f.flatten) // List(1, 2, 3, 4, 5, 6)

  println(f.map(_.map(_ * 2))) // List(List(2, 4), List(6, 8), List(10, 12))
  println(f.flatMap(_.map(_ * 2))) // List(2, 4, 6, 8, 10, 12)*/


  val txt = scala.io.Source.fromFile("custom/data/scala/hello.txt")
  // txt.foreach(print)
  val l2 = List(txt.mkString)
  println(l2, l2.length) // (List(hello,world,hello,hello),1)
  val l3 = l2.flatMap(_.split(","))
  println(l3, l3.length) // (List(hello, world, hello, hello),4)
  l2.flatMap(_.split(",")).map(x => (x, 1)).foreach(println)
  txt.close()


  /*val lines01 = Source.fromFile("custom/data/scala/a.txt").mkString
  val lines02 = Source.fromFile("custom/data/scala/b.txt").mkString
  var lines = List(lines01, lines02)
  println(lines.flatMap(_.split(" ")))
  println(lines.flatMap(_.split(" ")).map((_, 1)).map(_._2).sum)*/
}
