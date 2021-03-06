package com.coding.bigdata.scala.lesson04

object ConstructorApp {
  def main(args: Array[String]): Unit = {
    /*val person = new Person("zhangsan", 30)
    println(person.name + " : " + person.age + " : " + person.school)
    val person2 = new Person("lm", 18, "M")
    println(person.name + " : " + person.age + " : " + person.school + " : " + person2.gender)*/

    val student = new Student("lm", 18, "Computer")
    println(student.name + " : " + student.age + " : " + student.school + " : " + student.major)
    println(student)
  }
}

// 主构造器
class Person(val name: String, val age: Int) {
  println("Person Constructor enter...")

  val school = "ustc"
  var gender: String = _

  // 附属构造器
  def this(name: String, age: Int, gender: String) {
    this(name, age) // 附属构造器的第一行代码必须要调用主构造器或者其他附属构造器
    this.gender = gender
  }


  println("Person Constructor leave...")
}

class Student(name: String, age: Int, val major: String) extends Person(name, age) {
  println("Student Constructor enter...")

  // 重写属性
  override val school: String = "peking"

  // 重写方法
  override def toString: String = "Person:override def toString"

  println("Student Constructor leave...")

}
