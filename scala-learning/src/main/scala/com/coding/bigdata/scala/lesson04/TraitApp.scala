package com.coding.bigdata.scala.lesson04

/**
 * 接口多继承案例
 */
object PersonExample {
  def main(args: Array[String]): Unit = {
    val p1 = new Human("tom")
    val p2 = new Human("jack")
    p1.sayHello(p2.name)
    p1.makeFriends(p2)
  }

}

trait HelloTrait {
  def sayHello(name: String)
}

trait MakeFriendsTrait {
  def makeFriends(p: Human)
}


class Human(val name: String) extends HelloTrait with MakeFriendsTrait {
  override def sayHello(name: String): Unit = {
    println("hello," + name)

  }

  override def makeFriends(p: Human): Unit = {
    println("my name:" + name + ", your name:" + p.name)
  }
}
