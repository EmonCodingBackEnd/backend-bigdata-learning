package com.coding.bigdata.scala.lesson03

object LoopApp {

  def main(args: Array[String]): Unit = {
    /*println(1 to 10)
    println(1.to(10))

    println(Range(1, 10))
    println(Range(1, 10, 2))

    println(1 until 10)
    println(1.until(10))*/

    // 循环if守卫
    /*for (i <- 1 to 10 if i % 2 == 0) {
      println(i)
    }*/

    /*val courses = Array("Hadoop", "Spark SQL", "Spark Streaming", "Scala")
    for (elem <- courses) {
      println(elem)
    }
    courses.foreach(course => println(course))*/

    /*var (num, sum) = (100, 0)
    while (num > 0) {
      sum = sum + num
      num = num - 1
    }
    println(sum)*/

    // for推导式，得到一个Vector集合
    /*val ss = for (i <- 1 to 10) yield i * 2
    println(ss)*/
  }

}
