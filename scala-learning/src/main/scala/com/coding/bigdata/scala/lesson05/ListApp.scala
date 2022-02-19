package com.coding.bigdata.scala.lesson05

/*
 * List代表一个不可变的列表
 */
object ListApp extends App {
  /*val l = List(1, 2, 3, 4, 5)
  println(l.mkString(","))
  println(l.head) // 1
  println(l.tail) // List(2, 3, 4, 5)

  val l2 = 1 :: Nil
  println(l2.mkString(",")) // 1

  val l3 = 2 :: l2
  println(l3) // List(2, 1)*/

  val l4 = scala.collection.mutable.ListBuffer[Int]()
  l4 += 2
  l4 += (3, 4, 5)
  l4 ++= List(6, 7, 8, 9)
  l4 -= 2
  l4 -= 3
  l4 -= (1, 4) // List(5, 6, 7, 8, 9)
  println(l4.isEmpty) // false
  println(l4.head) // 5
  println(l4.tail) // ListBuffer(6, 7, 8, 9)
  println(l4.tail.head) // 6
  l4 --= List(5, 6, 7, 8)
  println(l4)

  l4.toList // 转换成普通列表

  def sum(nums: Int*): Int = {
    if (nums.isEmpty) {
      0
    } else {
      nums.head + sum(nums.tail: _*) // : _* 可以把 Seq 转换成 可变参数
    }
  }

  println(sum())
  println(sum(1, 2, 3, 4))

}
