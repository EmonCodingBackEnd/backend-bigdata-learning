package com.coding.bigdata.scala.lesson08

import java.io.File
import scala.language.implicitConversions

object ImplicitAspect {

  implicit def man2Superman(man: Man): Superman = new Superman(man.name)
  implicit def file2RichFile(file: File): RichFile = new RichFile(file)

}
