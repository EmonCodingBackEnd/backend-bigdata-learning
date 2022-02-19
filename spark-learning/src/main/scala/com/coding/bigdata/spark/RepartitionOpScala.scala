package com.coding.bigdata.spark

import com.coding.bigdata.common.{EnvScalaUtils, EnvUtils}
import org.apache.spark.SparkContext


/*
 * 需求：repartition的使用
 */
object RepartitionOpScala {

  def main(args: Array[String]): Unit = {
    // 第一步：创建SparkContext
    val conf = EnvScalaUtils.buildSparkConfByEnv(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Array(1, 2, 3, 4, 5), 2)

    // 重新设置RDD的分区数量为3，这个操作会产生shuffle
    // 也可以解决RDD中数据倾斜的问题
    dataRDD.repartition(3)
      .foreachPartition(it => {
        println("====================")
        it.foreach(println(_))
      })

    // 通过repartition可以控制输出数据产生的文件个数
    {
      val isWinLocal = true
      var path = "custom/data/spark/normal/output"
      if (!EnvUtils.isWin) {
        path = EnvUtils.toHDFSPath(path)
      }
      // 指定HDFS的路径信息即可，需要指定一个不存在的目录
      EnvUtils.checkOutputPath(path, isWinLocal)
      dataRDD.repartition(1).saveAsTextFile(path)
    }

    sc.stop()
  }
}
