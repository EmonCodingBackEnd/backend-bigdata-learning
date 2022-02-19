package com.coding.bigdata.spark.sql

import com.coding.bigdata.common.EnvScalaUtils
import org.apache.spark.sql.DataFrame

/*
 * 需求：DataFrame常见操作
 */
object DataFrameOpScala {

  def main(args: Array[String]): Unit = {
    // 创建SparkSession对象，里面包含SparkContext和SqlContext
    val sparkSession = EnvScalaUtils.buildSparkSessionByEnv(this.getClass.getSimpleName)

    // 读取json文件，获取DataFrame
    val dataFrame: DataFrame = sparkSession.read.json("custom/data/spark/sql/student.json")

    // 打印schema信息
    dataFrame.printSchema()

    // 默认显示所有数据，可以通过参数控制显示多少条
    dataFrame.show(2)

    // 查询数据中的指定字段信息
    dataFrame.select("name", "age").show()

    // 在使用select的时候可以对数据做一些操作，需要添加隐式转换函数，否则语法报错
    import sparkSession.implicits._
    dataFrame.select($"name", $"age" + 1).show()

    // 对数据进行过滤，需要添加隐式转换函数，否则报错
    dataFrame.filter($"age" > 18).show()
    // where底层调用的就是filter
    dataFrame.where($"age" > 18).show()

    // 对数据进行分组求和
    dataFrame.groupBy("age").count().show()

    // 排序显示
    dataFrame.orderBy("age").show()

    sparkSession.stop()
  }
}
