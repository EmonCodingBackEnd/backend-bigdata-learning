package com.coding.bigdata.spark.sql

import com.coding.bigdata.common.EnvScalaUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/*
 * 需求：使用json文件创建DataFrame
 */
object SqlDemoScala {


  def main(args: Array[String]): Unit = {
    // 创建SparkSession对象，里面包含SparkContext和SqlContext
    val sparkSession = EnvScalaUtils.buildSparkSessionByEnv(this.getClass.getSimpleName)

    // 读取json文件，获取DataFrame
    val dataFrame: DataFrame = sparkSession.read.json("custom/data/spark/sql/student.json")
    // 可以等效转换为DataSet
    val dataSet: Dataset[Row] = dataFrame.as("stu")

    // 查看DataFrame中的数据
    dataFrame.show()

    sparkSession.stop()
  }
}
