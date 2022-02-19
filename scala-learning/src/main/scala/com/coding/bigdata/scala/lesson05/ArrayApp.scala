package com.coding.bigdata.scala.lesson05

object ArrayApp extends App {
  /*println("test")
  val a = new Array[String](5)
  println(a.length)
  a(1) = "hello"
  println(a(1))

  val b = Array("hadoop", "spark", "storm")
  println(b(1))
  b(1) = "streaming"
  println(b(1))

  val c = Array(2, 3, 4, 5, 6, 7, 8, 9)
  println(c.sum)
  println(c.mkString(","))
  println(c.mkString("<", ",", ">"))*/

  // 可变数组
  val d = scala.collection.mutable.ArrayBuffer[Int]()
  d += 1
  d += 2
  d += (3, 4, 5)
  d ++= Array(6, 7, 8)
  d.insert(0, 0)
  d.remove(1) // 移除第二个元素 => 0,2,3,4,5,6,7,8
  d.remove(0, 3) // 移除第1到2个元素 => 4,5,6,7,8
  d.trimEnd(2) // 4,5,6
  println(d.mkString(","))

  for (elem <- 0 until d.length) {
    println(d(elem))
  }

  for (elem <- d.indices) {
    println(d(elem))
  }

  for (elem <- d) {
    println(elem)
  }

  for (elem <- (0 until d.length).reverse) {
    println(d(elem))
  }

  d.toArray // 转换成普通数组
}

/*
 * 集合体系：Scala中的集合是分为可变和不可变集合两类的。
 * 可变集合：scala.collection.mutable
 * 不可变集合：scala.collection.immutable【默认】
 * Iterable
 *    Set：代表一个没有重复元素的集合，分为可变集合和不可变集合，默认情况下使用的是不可变集合。
 *        HashSet：包含可变和不可变两种。
 *            这个集合的特点是，集合中的元素不重复、无序。
 *        LinkedHashSet：只有可变的，没有不可变的。
 *            这个集合的特点是，集合中的元素不重复、有序，它会用一个链表维护插入顺序，可以保证集合中的元素是有序的。
 *        SortedSet：分为可变和不可变集合。
 *            这个集合的特点是，集合中的元素不重复、有序，它会自动根据元素来进行排序。
 *    Seq
 *        List：代表一个不可变的列表
 *        Buffer
 *            ArrayBuffer
 *            ListBuffer
 *        Range
 *    Map：是一种可迭代的键值对（key/value）结构，分为可变和不可变，默认情况下使用的是不可变Map。
 *        HashMap：包含可变与不可变。
 *            是一个按照key的hash值进行排序存储的map
 *        SortedMap：包含可变和不可变的
 *            可以自动对map中的key进行排序【有序map】
 *        LinkedHashMap：是可变的
 *            可以记住插入的key-value的顺序
 *  Tuple和Array
 *      Array：长度不可变
 *      Tuple：称之为元组，它与Array类型，都是不可变的，但与列表不同的是元组可以包含不同类型的元素。
 *            目前Scala支持的元组最大长度为22，对于更大长度可以使用集合或数组。
 */