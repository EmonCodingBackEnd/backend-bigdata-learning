package com.coding.bigdata.spark

import com.coding.bigdata.common.EnvScalaUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/*
 * 需求：使用广播变量
 */
object BroadcastOpScala {

  def main(args: Array[String]): Unit = {
    // 第一步：创建SparkContext
    val conf = EnvScalaUtils.buildSparkConfByEnv(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Array(1, 2, 3, 4, 5))

    val variable = 2

    // 直接使用普通外部变量，会拷贝到task
    //    dataRDD.map(_ * variable)

    // 1、定义广播变量
    val variableBroadcast: Broadcast[Int] = sc.broadcast(variable)

    // 2、使用广播变量，调用其他value方法
    dataRDD.map(_ * variableBroadcast.value).foreach(println(_))

    sc.stop()
  }
}
