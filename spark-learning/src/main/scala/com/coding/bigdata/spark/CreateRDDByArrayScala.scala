package com.coding.bigdata.spark

import com.coding.bigdata.common.EnvScalaUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/*
 * 需求：使用集合创建RDD
 */
object CreateRDDByArrayScala {

  def main(args: Array[String]): Unit = {
    // 第一步：创建SparkContext
    val conf = EnvScalaUtils.buildSparkConfByEnv(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    // 创建集合
    val arr = Array(1, 2, 3, 4, 5, 6)
    // 基于集合创建RDD
    val rdd: RDD[Int] = sc.parallelize(arr)
    // 对集合中的数据求和
    val sum: Int = rdd.reduce(_ + _) // 这里创建的任务会在集群中执行

    // 注意：这行println代码是在driver进程中执行的
    println(sum)
  }
}
