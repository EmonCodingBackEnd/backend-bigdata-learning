package com.coding.bigdata.flink.window

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/*
 * 需求：自定义MyWindow的使用
 * 1：滚动窗口
 *
 * 前提：在启动该程序之前，先在指定主机emon启动命令： nc -lk 9000
 * 等启动程序后，在emon主机终端输入： hello you hello me
 */
object MyTimeWindowOpScala {

  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text: DataStream[String] = env.socketTextStream("emon", 9000)

    // 自定义MyTimeWindow滚动窗口：每隔10秒计算一次前10秒时间窗口内的数据
    text.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      // 窗口大小
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .sum(1)
      .print()

    env.execute(this.getClass.getSimpleName)

  }
}
