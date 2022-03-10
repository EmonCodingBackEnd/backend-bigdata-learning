package com.coding.bigdata.spark.stream

import com.coding.bigdata.common.EnvScalaUtils
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
 * 需求：通过socket模拟产生数据，实时计算数据中单词出现的次数。
 * 环境准备：
 * 1、在emon机器打开socket
 * nc -lk 9000
 * 2、启动程序
 * 3、在nc -lk 9000命令行
 * 在命令行窗口输入数据，比如：hello flink
 */
object StreamWordCountScala {

  def main(args: Array[String]): Unit = {
    // 第一步：创建SparkConf
    val conf = EnvScalaUtils.buildSparkConfByEnv(this.getClass.getSimpleName)

    // 创建StreamingContext，指定数据处理间隔为5秒
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    // 通过socket获取实时产生的数据
    val linesRDD: ReceiverInputDStream[String] = ssc.socketTextStream("emon", 9000)

    // 对接收到的数据使用空格进行切割，转换成单个单词
    val wordsRDD: DStream[String] = linesRDD.flatMap(_.split(" "))

    // 把每个单词转换成tuple2的形式
    val tupRDD: DStream[(String, Int)] = wordsRDD.map((_, 1))

    // 执行reduceByKey操作
    val wordCountRDD: DStream[(String, Int)] = tupRDD.reduceByKey(_ + _)

    // 将结果数据打印到控制台
    wordCountRDD.print()

    // 启动任务
    ssc.start()

    // 等待任务停止
    ssc.awaitTermination()
  }
}
