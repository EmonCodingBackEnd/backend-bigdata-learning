package com.coding.bigdata.flink.window

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
 * CountWindow的使用
 * 1：滚动窗口
 * 2：滑动窗口
 *
 * 前提：在启动该程序之前，先在指定主机emon启动命令： nc -lk 9000
 * 等启动程序后，在emon主机终端输入： hello you hello me
 */
object CountWindowOpScala {

  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text: DataStream[String] = env.socketTextStream("emon", 9000)

    // CountWindow之滚动窗口：每隔5个元素计算一次前5个元素：非常注意，由于是分组了，表示分组内每隔元素的次数达到5个才会输出一次
    /*text.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      // 窗口大小
      .countWindow(5)
      .sum(1)
      .print()*/

    // CountWindow之滑动窗口：每隔1个元素计算一次前5个元素
    text.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      // 第一个参数：窗口大小；第二个参数：滑动间隔
      .countWindow(5, 1)
      .sum(1)
      .print()

    env.execute(this.getClass.getSimpleName)

  }
}
