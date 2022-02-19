package com.coding.bigdata.scala.lesson05

object SetApp extends App {
  val set = Set(1, 2, 2, 1, 4, 3)
  println(set) // Set(1, 2, 4, 3)

  val set2 = scala.collection.mutable.Set[Int]()
  set2 += 1
  set2 += (1, 1)
  set2 ++= Set(1, 2, 3)
  set2 -= (2)
  println(set2)
}
