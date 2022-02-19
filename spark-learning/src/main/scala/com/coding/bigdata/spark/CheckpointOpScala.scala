package com.coding.bigdata.spark

import com.coding.bigdata.common.{EnvScalaUtils, EnvUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/*
 * 需求：checkpoint的使用
 *
 * 任务提交脚本：
 * 创建脚本： [emon@emon ~]$ vim /home/emon/bigdata/spark/shell/checkpointOpScala.sh
 * 脚本内容：如下：
spark-submit \
--class com.coding.bigdata.spark.CheckpointOpScala \
--master yarn \
--deploy-mode cluster \
--executor-memory 1G \
--num-executors 1 \
~/bigdata/spark/lib/spark-learning-1.0-SNAPSHOT-jar-with-dependencies.jar
 * 修改脚本可执行权限：[emon@emon ~]$ chmod u+x /home/emon/bigdata/spark/shell/checkpointOpScala.sh
 * 执行脚本：[emon@emon ~]$ sh -x /home/emon/bigdata/spark/shell/checkpointOpScala.sh
 */
object CheckpointOpScala {


  def main(args: Array[String]): Unit = {
    // 第一步：创建SparkContext
    val conf = EnvScalaUtils.buildSparkConfByEnv(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    var isWinLocal = true
    var checkpointDir = "checkpoint/chk001"
    var filePath = "custom/data/mr/skew/input/hello_10000000.dat"
    var outputPath = "custom/data/spark/chk001/output"
    if (!EnvUtils.isWin) {
      isWinLocal = false
      checkpointDir = EnvUtils.toHDFSPath(checkpointDir)
      filePath = EnvUtils.toHDFSPath(filePath)
      outputPath = EnvUtils.toHDFSPath(outputPath)
    }
    // 清理checkpoint目录：非必须
    EnvUtils.checkOutputPath(checkpointDir, isWinLocal)
    // 指定HDFS的路径信息即可，需要指定一个不存在的目录
    EnvUtils.checkOutputPath(outputPath, isWinLocal)

    // 1、设置checkpoint目录
    sc.setCheckpointDir(checkpointDir)

    val dataRDD: RDD[String] = sc.textFile(filePath)
      // 执行持久化
      .persist(StorageLevel.DISK_ONLY)

    // 2、对RDD执行checkpoint操作
    dataRDD.checkpoint()

    dataRDD.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(outputPath)

    // while循环是为了保证程序不结束，方便在本地查看4040页面中的storage信息
    /*while (true) {
      ;
    }*/
    sc.stop()
  }
}
