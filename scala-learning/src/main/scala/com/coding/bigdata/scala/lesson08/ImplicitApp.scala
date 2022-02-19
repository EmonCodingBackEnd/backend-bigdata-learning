package com.coding.bigdata.scala.lesson08

import java.io.File
import scala.language.implicitConversions

/**
 * 隐式转换
 * 隐式参数：指的是在函数或者方法中，定义一个用implicit修饰的参数，此时Scala会尝试找到一个指定类型的，
 * 用implicit修饰的对象，即隐式值，并注入参数
 * 隐式类：就是对类增加implicit限定的类，其作用主要是对类的加强！
 */
object ImplicitApp extends App {


  // 让人变超人
  {
    // 定义隐式转换函数即可
    implicit def man2Superman(man: Man): Superman = new Superman(man.name)

    val man = new Man("lm")
    man.eat()
    man.fly()
  }


  // 让狗抓老鼠
  {
    implicit def object2Cat(obj: Object): Cat = {
      if (obj.getClass == classOf[Dog]) {
        val dog = obj.asInstanceOf[Dog]
        new Cat(dog.name)
      }
      else {
        null
      }
    }

    val dog = new Dog("dog1")
    dog.catchMouse()
  }


  // 增强文件处理器
  {
    import ImplicitAspect.file2RichFile

    val file = new File("custom/data/scala/hello.txt")
    println(file.read())
  }


  // 隐式参数
  {
    def testParam(implicit name: String): Unit = {
      println(name + "~~~~~~~~")
    }

    // 这里字符串类型的变量名，不必是name
    implicit val name2 = "implicit_name2"
    testParam // implicit_name~~~~~~~~
    testParam("lm") // lm~~~~~~~~
  }
}


class Man(val name: String) {
  def eat(): Unit = {
    println(s"man[ $name ] eat ......")
  }
}

class Superman(val name: String) {
  def fly(): Unit = {
    println(s"superman[ $name ] fly ......")
  }
}

class RichFile(val file: File) {
  def read() = {
    val fs = scala.io.Source.fromFile(file.getPath)
    fs.mkString
  }
}


class Cat(val name: String) {
  def catchMouse(): Unit = {
    println(name + " catch mouse")
  }
}

class Dog(val name: String)
