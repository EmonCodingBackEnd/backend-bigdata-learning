package com.coding.bigdata.spark.sql

import com.coding.bigdata.common.EnvScalaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/*
 * 需求：使用编程方式实现RDD转换为DataFrame
 */
object RDDToDataFrameByProgramScala {

  def main(args: Array[String]): Unit = {
    // 创建SparkSession对象，里面包含SparkContext和SqlContext
    val sparkSession = EnvScalaUtils.buildSparkSessionByEnv(this.getClass.getSimpleName)

    // 获取SparkContext
    val sc = sparkSession.sparkContext

    val dataRDD: RDD[(String, Int)] = sc.parallelize(Array(("jack", 18), ("tom", 20), ("jessic", 30)))

    // 组装rowRDD
    val rowRDD: RDD[Row] = dataRDD.map(tup => Row(tup._1, tup._2))

    // 指定元数据信息【这个元数据信息就可以动态从外部获取了，比较灵活】
    val schema = StructType(Array(
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true)
    ))

    // 组装DataFrame
    val stuDF = sparkSession.createDataFrame(rowRDD, schema)

    // 下面就可以通过DataFrame的方式操作dataRDD中的数据了
    stuDF.createOrReplaceTempView("student")

    // 执行sql查询
    val resDF = sparkSession.sql(
      """
        | select name, age from student where age > 18
        |""".stripMargin)

    // 将DataFrame转化为RDD
    val resRDD: RDD[Row] = resDF.rdd

    // 从row中取数据，封装成student，打印到控制台
    resRDD.map(row => (row(0).toString, row(1).toString.toInt))
      .collect()
      .foreach(println(_))

    // 使用row的getAs方法，获取指定列名的值
    resRDD.map(row => (row.getAs[String]("name"), row.getAs[Int]("age")))
      .collect().foreach(println(_))

    sparkSession.stop()
  }
}
