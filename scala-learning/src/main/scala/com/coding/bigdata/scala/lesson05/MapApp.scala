package com.coding.bigdata.scala.lesson05

object MapApp extends App {

  /*val ages = Map("jack" -> 30, "tom" -> 25, "jessic" -> 23)
  println(ages)
  println(ages("jack"))*/

  /*val ages = Map(("jack", 30), ("tom", 25), ("jessic" -> 23))
  println(ages("jessic"))*/

  /*val ages = scala.collection.mutable.Map("jack" -> 30, "tom" -> 25, "jessic" -> 23)
  println(ages)
  ages.put("lm", 18)
  ages += (("wh", 16))
  ages += ("ls" -> 15)
  ages -= "jessic"
  println(ages)

  println(ages.getOrElse("xzm", 1))*/

  /*// 变成普通不可变的Map
  var ll = ages.toMap
  println(ll)*/

  /*for ((key, value) <- ages) {
    println(key, value)
  }*/

  /*for (key <- ages.keySet) {
    println(key, ages(key))
  }*/

  /*for (elem <- ages.values) {
    println(elem)
  }*/

  /*for (elem <- ages.keys) {
    println(elem)
  }*/

  /*val ages = scala.collection.immutable.SortedMap("b" -> 30, "a" -> 15, "c" -> 25)
  println(ages)*/

  val ages = scala.collection.mutable.LinkedHashMap("b" -> 30, "a" -> 15, "c" -> 25)
  ages("d") = 20
  ages += ("e" -> 1)
  println(ages)
}
