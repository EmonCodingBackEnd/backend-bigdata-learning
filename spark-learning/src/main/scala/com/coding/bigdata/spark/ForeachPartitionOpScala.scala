package com.coding.bigdata.spark

import com.coding.bigdata.common.EnvScalaUtils
import org.apache.spark.SparkContext

/*
 * 需求：foreachPartition的使用
 */
object ForeachPartitionOpScala {

  def main(args: Array[String]): Unit = {
    // 第一步：创建SparkContext
    val conf = EnvScalaUtils.buildSparkConfByEnv(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Array(1, 2, 3, 4, 5), 2)
    // foreachPartition：一次处理一个分区的数据，foreachPartition和mapPartitions的特性是一样的，
    // 唯一的区别就是mapPartitions是transformation操作（不会立即执行），foreachPartition是action操作（会立即执行）
    dataRDD.foreachPartition(it => {
      println("====================")
      // 在此处获取数据库链接
      it.foreach(item => {
        // 在这里使用数据库链接
        println(item)
      })
      // 在这里关闭数据库链接
    })

    sc.stop()
  }
}
