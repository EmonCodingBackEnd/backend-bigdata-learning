package com.coding.bigdata.scala.lesson09

import java.sql.DriverManager

object MySQLAPP extends App {
  val url = "jdbc:mysql://repo.emon.vip:3306/mysql"
  val username = "flyin"
  val password = "Flyin@123"

  val connection = DriverManager.getConnection(url, username, password)
  try {
    // make the connection
    classOf[com.mysql.jdbc.Driver]

    // create the statement and run the select query
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery("select user, host from user")
    while (resultSet.next()) {
      val user = resultSet.getString("user")
      val host = resultSet.getString("host")
      println(s"$user : $host")
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    if (connection != null) {
      connection.close()
    }
  }
}
