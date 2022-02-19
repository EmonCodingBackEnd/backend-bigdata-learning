package com.coding.bigdata.scala.lesson07

import scala.util.Random

/**
 * 偏函数：被包在花括号内没有match的一组case语句
 */
object PartitalFunctionApp extends App {
  val names = Array("Akiho Yoshizawa", "YuiHatano", "Aoi Sola")
  val name = names(Random.nextInt(names.length))
  println(name)

  name match {
    case "Akiho Yoshizawa" => println("吉老师")
    case "YuiHatano" => println("波老师")
    case "Aoi Sola" => println("苍老师")
    case _ => println("不知名老师")
  }

  // A 输入参数类型 B输出参数类型
  def sayChinese: PartialFunction[String, String] = {
    case "Akiho Yoshizawa" => "吉老师"
    case "YuiHatano" => "波老师"
    case "Aoi Sola" => "苍老师"
    case _ => "不知名老师"
  }

  println(sayChinese("Akiho Yoshizawa"))
}
