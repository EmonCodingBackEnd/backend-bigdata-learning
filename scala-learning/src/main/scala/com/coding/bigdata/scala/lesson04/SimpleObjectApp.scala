package com.coding.bigdata.scala.lesson04

object SimpleObjectApp {
  def main(args: Array[String]): Unit = {
    val person = new People()
    person.name = "Messi"
    println(person.name + " " + person.age)
    println("invoke eat method: " + person.eat)
    person.watchFootbal("Barcelona")

    person.printInfo()
  }
}

class People {
  var name: String = _ // _ 是占位符
  val age: Int = 10

  private[this] val gender = "male" // 对于对象私有的字段，Scala不生成getter/setter方法；
  private val gender2 = "male2" // 对于类私有的字段，Scala生成私有的getter/setter方法

  def printInfo(): Unit = {
    println("gender: " + gender + "gender2: " + gender2)
  }

  def eat(): String = {
    name + " eat......"
  }

  def watchFootbal(teamName: String): Unit = {
    println(name + " is watching match of " + teamName)
  }

  def someInfo(): Unit = {
    val person = new People()
    println(person.name + " " + person.age + " "
      // + person.gender // 只能在类内部使用，对象都不能直接使用
      + " " + person.gender2 // 在类内部和对象都可以使用
    )
  }
}

