package com.coding.bigdata.spark

import com.coding.bigdata.common.EnvScalaUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/*
 * 需求：使用文件创建RDD
 */
object CreateRDDByFileScala {

  def main(args: Array[String]): Unit = {
    // 第一步：创建SparkContext
    val conf = EnvScalaUtils.buildSparkConfByEnv(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    var path = "custom/data/spark/hello.txt"
    if (args.length == 1) {
      path = args(0)
    }

    // 读取文件数据，可以在 textFile 中指定生成的RDD的分区数量
    val linesRDD: RDD[String] = sc.textFile(path, 2)

    // 获取每一行数据的长度，计算文件内数据的总长度
    val length: Int = linesRDD.map(_.length).reduce(_ + _)

    // 注意：这行println代码是在driver进程中执行的
    println(length)
  }
}
