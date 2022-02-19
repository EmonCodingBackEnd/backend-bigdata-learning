package com.coding.bigdata.scala.lesson06

import java.io.{FileNotFoundException, IOException}

/**
 * 模式匹配：
 * Java： 对一个值进行条件判断，返回针对不同的条件进行不同的处理
 *
 * Scala：
 * 变量 match {
 * case value1 => 代码1
 * case value2 => 代码2
 * ......
 * case _ => 代码N
 */
object MatchApp extends App {

  /*val names = Array("Akiho Yoshizawa", "YuiHatano", "Aoi Sola")
  val name = names(Random.nextInt(names.length))
  println(name)

  name match {
    case "Akiho Yoshizawa" => println("吉老师")
    case "YuiHatano" => println("波老师")
    case "Aoi Sola" => println("苍老师")
    case _ => println("不知名老师")
  }*/


  /*def judgeGrade(grade: String): Unit = {
    grade match {
      case "A" => println("Excellent...")
      case "B" => println("Good...")
      case "C" => println("Just so so...")
      case _ => println("You need work harder...")
    }
  }*/


  /*def judgeGrade(grade: String, name: String): Unit = {
    grade match {
      case "A" => println("Excellent...")
      case "B" => println("Good...")
      case "C" => println("Just so so...")
      case _ if (name == "lisi") => println(name + " you are a good boy, but...")
      case _ => println(" You need work harder...")
    }
  }

  judgeGrade("A", "zhangsan")
  judgeGrade("D", "wangwu")
  judgeGrade("A", "lisi")
  judgeGrade("D", "lisi")
  judgeGrade("B", "xiaohong")*/


  /*def greeting(array: Array[String]): Unit = {
    array match {
      case Array("zhangsan") => println("Hi:zhangsan")
      case Array(x, y) => println("Hi:" + x + "," + y)
      case Array("zhangsan", _*) => println("Hi:zhangsan and other friends...")
      case _ => println("Hi:everybody...")
    }
  }

  greeting(Array("zhangsan")) // Hi:zhangsan
  greeting(Array("lisi")) // Hi:everybody...
  greeting(Array("zhangsan", "xiaohong")) // Hi:zhangsan,xiaohong
  greeting(Array("zhangsan", "xiaohong", "wangwu")) // Hi:zhangsan and other friends...
  greeting(Array("wangwu", "xiaohong", "lisi")) // Hi:everybody...*/


  /*def greeting(array: List[String]): Unit = {
    array match {
      case "zhangsan" :: Nil => println("Hi:zhangsan")
      case x :: y :: Nil => println("Hi:" + x + "," + y)
      case "zhangsan" :: tail => println("Hi:zhangsan and other friends...")
      case _ => println("Hi:everybody...")
    }
  }

  greeting(List("zhangsan")) // Hi:zhangsan
  greeting(List("lisi")) // Hi:everybody...
  greeting(List("zhangsan", "xiaohong")) // Hi:zhangsan,xiaohong
  greeting(List("zhangsan", "xiaohong", "wangwu")) // Hi:zhangsan and other friends...
  greeting(List("wangwu", "xiaohong", "lisi")) // Hi:everybody...*/


  /*def matchType(obj: Any): Unit = {
    obj match {
      case x: Int => println("Int")
      case x: String => println("String")
      case x: Map[_, _] => x.foreach(println)
      case _ => println("other type")
    }
  }

  matchType(1)
  matchType("1")
  matchType(1l)
  matchType(1f)
  matchType(Map("name" -> "lm"))*/


  def caseClassMatch(person: Person): Unit = {
    person match {
      case CTO(name, floor) => println("CTO name is: " + name + " , floor is: " + floor)
      case Employee(name, floor) => println("Employee name is: " + name + " , floor is: " + floor)
      case _ => println("other")
    }
  }

  class Person

  case class CTO(name: String, floor: String) extends Person

  case class Employee(name: String, floor: String) extends Person

  case class Other(name: String) extends Person

  caseClassMatch(CTO("lm", "22"))
  caseClassMatch(Employee("zhangsan", "22"))
  caseClassMatch(Other("lisi"))


  /*def processException(e: Exception): Unit = {
    e match {
      case e: FileNotFoundException => println("FileNotFoundException=" + e)
      case e: IOException => println("IOException=" + e)
      case _: Exception => println("Exception=" + e)
    }
  }

  processException(new FileNotFoundException())
  processException(new IOException())
  processException(new IndexOutOfBoundsException())*/
}
