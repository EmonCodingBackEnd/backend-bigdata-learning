package com.coding.bigdata.spark

import com.coding.bigdata.common.{EnvScalaUtils, EnvUtils}
import org.apache.spark.SparkContext

/*
 * 需求：测试内存占用情况
 */
object TestMemoryScala {

  def main(args: Array[String]): Unit = {
    // 第一步：创建SparkContext
    val conf = EnvScalaUtils.buildSparkConfByEnv(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    println(sc.defaultParallelism)

    var path = "custom/data/mr/skew/input/hello_10000000.dat"
    if (!EnvUtils.isWin) {
      path = EnvUtils.toHDFSPath(path)
    }
    // 注意cache的用法和位置，cache默认是基于内存的持久化
    val dataRDD = sc.textFile(path).cache()
    val count = dataRDD.count()
    println(count)

    // while循环是为了保证程序不结束，方便在本地查看4040页面中的storage信息
    while (true) {
      ;
    }
    sc.stop()
  }
}
