package com.coding.bigdata.spark.sql

import com.coding.bigdata.common.EnvScalaUtils

/*
 * 需求：使用sql操作DataFrame
 */
object DataFrameSqlScala {

  def main(args: Array[String]): Unit = {
    // 创建SparkSession对象，里面包含SparkContext和SqlContext
    val sparkSession = EnvScalaUtils.buildSparkSessionByEnv(this.getClass.getSimpleName)

    // 读取json文件，获取DataFrame
    val dataFrame = sparkSession.read.json("custom/data/spark/sql/student.json")

    // 将DataFrame注册为一个临时表
    dataFrame.createOrReplaceTempView("student")

    // 使用sql查询临时表中的数据
    sparkSession.sql(
      """
        | select age,count(*) as num from student group by age
        |""".stripMargin
    ).show()

    sparkSession.stop()
  }
}
