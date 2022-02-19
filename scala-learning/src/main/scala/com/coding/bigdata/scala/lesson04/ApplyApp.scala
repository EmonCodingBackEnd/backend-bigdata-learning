package com.coding.bigdata.scala.lesson04

object ApplyApp {
  def main(args: Array[String]): Unit = {
    /*for (i <- 1 to 10) {
      ApplyTest.incr
    }
    println(ApplyTest.count) // 10 说明object本身就是一个单例对象*/

    val b = ApplyTest() // ==> Object.apply

    println("~~~~~~~~~~")
    val c = new ApplyTest()
    println(c)
    c() // ==> Class.apply
  }
}

/**
 * 伴生类和伴生对象
 * 如果有一个class，还有一个与class同名的object
 * 那么就称这个object是class的伴生对象，class是object的伴生类
 * 伴生类和伴生对象必须存在于一个.scala文件
 */
class ApplyTest {
  def apply() = {
    println("Class ApplyTest apply...")
  }
}

object ApplyTest {
  println("Object ApplyTest enter...")
  var count = 0

  def incr = {
    count = count + 1
  }

  // 最佳实践：在Object的apply方法中去new Class
  def apply() = {
    println("Object ApplyTest apply...")
    // 在object中的apply中new class
    new ApplyTest()
  }

  println("Object ApplyTest leave...")
}
