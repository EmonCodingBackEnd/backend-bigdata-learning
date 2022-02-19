package com.coding.bigdata.scala.lesson09

import java.io.{FileInputStream, InputStreamReader}
import scala.xml.XML

object XMLApp extends App {

  //  loadXML()
  readXMLAttr()

  def loadXML(): Unit = {
    /*val xml = XML.load(this.getClass.getClassLoader.getResource("test.xml"))
    println(xml)*/

    /*val xml = XML.load(new FileInputStream("C:\\Job\\JobResource\\IdeaProjects\\Idea2020\\backend-scala-learning\\src\\main\\resources\\test.xml"))
    println(xml)*/

    val xml = XML.load(new InputStreamReader(new FileInputStream("C:\\Job\\JobResource\\IdeaProjects\\Idea2020\\backend-scala-learning\\src\\main\\resources\\test.xml")))
    println(xml)
  }

  def readXMLAttr(): Unit = {
    val xml = XML.load(this.getClass.getClassLoader.getResource("lm.xml"))
    // println(xml)

    // header/field
    /*val headerField = xml \ "header" \ "field"
    println(headerField)*/


    // all field
    /*val fields = xml \\ "field"
    for (elem <- fields) {
      println(elem)
    }*/

    // header/field/name
    // val fieldAttributes = (xml \ "header" \ "field").map(_ \ "@name")
    /*val fieldAttributes = (xml \ "header" \ "field" \\ "@name")
    for (elem <- fieldAttributes) {
      println(elem)
    }*/

    // name = "Logon" message
    // val filters = (xml \\ "message").filter(_.attribute("name").exists(_.text.equals("Logon")))
    /*val filters = (xml \\ "message").filter(x => ((x \ "@name").text).equals("Logon"))
    for (elem <- filters) {
      println(elem)
    }*/

    // header/field/name
    (xml \ "header" \ "field").map(x => (x \ "@name", x.text, x \ "@required")).foreach(println)
  }

}
