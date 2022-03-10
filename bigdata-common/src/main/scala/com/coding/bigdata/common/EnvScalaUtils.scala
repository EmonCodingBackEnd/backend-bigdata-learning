package com.coding.bigdata.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object EnvScalaUtils {

  def buildSparkConfByEnv(/*isWinLocal: Boolean*/ appName: String): SparkConf = {
    val sparkConf = new SparkConf()
    if (EnvUtils.isWin) {
      System.setProperty(
        "hadoop.home.dir", "C:\\Job\\JobSoftware\\hadoop-common-2.2.0-bin-master");
      System.setProperty("HADOOP_USER_NAME", "emon")
      sparkConf.setAppName(appName) // 设置任务名称
        /*
         * 解决bug
         * A master URL must be set in your configuration
         * 注意：此处的local[2]在SparkStreaming中表示启动2个进程，一个进程负责读取数据源的数据，一个进程负责处理数据
         */
        .setMaster("local[2]") // local[2]表示本地执行，使用2个工作线程的 StreamingContext；local表示1个工作线程；local[*]自动获取
        /*
         * 解决bug
         * java.lang.IllegalArgumentException: System memory 259522560 must be at least 471859200. Please increase heap size using the --driver-memory option or spark.driver.memory in Spark configuration.
         */
        .set("spark.testing.memory", 512 * 1024 * 1024 + "")
    }
    sparkConf
  }

  def buildSparkSessionByEnv(appName: String): SparkSession = {
    val builder = SparkSession.builder()
    // Logger.getLogger("org").setLevel(Level.WARN)
    if (EnvUtils.isWin) {
      System.setProperty(
        "hadoop.home.dir", "C:\\Job\\JobSoftware\\hadoop-common-2.2.0-bin-master")

      builder.appName(appName)
        .master("local[2]") // local[2]表示本地执行，使用2个工作线程的 StreamingContext；local表示1个工作线程；local[*]自动获取
        .config("spark.testing.memory", 512 * 1024 * 1024 + "")
    }

    val sparkSession = builder.getOrCreate()

    if (EnvUtils.isWin) {
      // 默认分区200个，对于小作业来说，大部分时间花费到了任务调度上；设置初始分区数的1.5-2倍
      val key = "spark.sql.shuffle.partitions"
      sparkSession.conf.set(key, 2)
      println(s"$key = " + sparkSession.conf.get(key))
    }

    sparkSession
  }
}


