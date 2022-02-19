package com.coding.bigdata.scala.lesson07

object StringApp extends App {
  val s = "Hello"
  val name = "lm"
  println(s + ":" + name)

  println(s"Hello:$name")
  val team = "AC Milan"

  // 插值表达式
  println(s"Hello:$name, Welcome to $team")

  // 多行字符串
  val b =
    """
      |这是一个多行字符串
      |hello
      |world
      |lm
      |""".stripMargin
  println(b)

  // 多行插值字符串
  val c =
    s"""
       |Hello
       |$name
       |Welcome to AC Milan
       |""".stripMargin
  println(c)
}
