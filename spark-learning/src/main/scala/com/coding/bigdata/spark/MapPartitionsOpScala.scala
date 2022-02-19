package com.coding.bigdata.spark

import com.coding.bigdata.common.EnvScalaUtils
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

/*
 * 需求：mapPartitions的使用
 */
object MapPartitionsOpScala {

  def main(args: Array[String]): Unit = {
    // 第一步：创建SparkContext
    val conf = EnvScalaUtils.buildSparkConfByEnv(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    // 设置分区数量为2
    val dataRDD = sc.parallelize(Array(1, 2, 3, 4, 5), 2)

    // map算子一次处理一条数据
    /*val sum = dataRDD.map(item => {
      println("====================")
      item * 2
    }).reduce(_ + _)*/

    // mapPartitions算子一次处理一个分区的数据
    val sum = dataRDD.mapPartitions(it => {
      // 建议针对初始化链接之类的操作，使用mapPartitions，放在mapPartitions内部
      // 例如：创建数据库链接，使用mapPartitions可以减少链接创建的次数，提高性能
      // 注意：创建数据库链接的代码建议放在此处，不要放在Driver端或者it.foreach内部
      // 数据库链接无法序列化，如果在Driver端创建，无法传递到对应的task中，所以算子在执行的时候会报错
      // 数据库链接放在it.foreach()内部还是会创建多个连接，和使用map算子的效果是一样的
      println("====================")
      val result = new ArrayBuffer[Int]()
      // 这个foreach是调用的scala里面的函数
      it.foreach(item => {
        result += item * 2
      })
      result.iterator
    }).reduce(_ + _)

    println("sum:" + sum)

    sc.stop()
  }

}
