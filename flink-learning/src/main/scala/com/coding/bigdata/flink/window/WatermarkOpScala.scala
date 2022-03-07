package com.coding.bigdata.flink.window

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.time.Duration
import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

/*
 * Watermark+EventTime解决数据乱序问题
 */
object WatermarkOpScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置使用数据产生的时间：EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置全局并行度为1
    env.setParallelism(1)
    // 设置自动周期性的产生Watermark，默认值为200毫秒
    env.getConfig.setAutoWatermarkInterval(200)

    val text: DataStream[String] = env.socketTextStream("emon", 9000)

    // 将数据转换为tuple2的形式
    // 第一列表示具体的数据，第二列表示是数据产生的时间戳
    import org.apache.flink.api.scala._
    val tupStream: DataStream[(String, Long)] = text.map(line => {
      val arr = line.split(",")
      (arr(0), arr(1).toLong)
    })

    // 分配（提取）时间戳和Watermark
    val waterMarkStream: DataStream[(String, Long)] = tupStream.assignTimestampsAndWatermarks(
      // 最大允许的数据乱序时间 10s
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[Tuple2[String, Long]] {
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            var currentMaxTimestamp = 0L
            var currentWatermark = 0L

            // 从数据流中抽取时间戳作为EventTime
            override def extractTimestamp(ele: (String, Long), recordTimestamp: Long): Long = {
              val timestamp = ele._2
              currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)

              // 计算当前的Watermark，为了打印出来，方便观察数据，没有别的作用
              currentWatermark = currentMaxTimestamp - 10000L
              // 此println语句仅仅是为了在学习阶段观察数据的变化
              println(
                "key:" + ele._1
                  + ",eventTime:[" + ele._2 + "|" + sdf.format(ele._2) + "]"
                  + ",currentMaxTimestamp:[" + currentMaxTimestamp + "|" + sdf.format(currentMaxTimestamp) + "]"
                  + ",currentWatermark:[" + currentWatermark + "|" + sdf.format(currentWatermark) + "]"
              )

              timestamp
            }

          }
        )
    )

    waterMarkStream.keyBy(0)
      // 按照消息的EventTime分配窗口，和调用TimeWindow效果一样
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      // 使用全量聚合的方式处理Window中的数据
      .apply(
        /*
         * 知识点：
         * 1、apply方法中，可以添加 WindowFunction 对象，会将该窗口中所有的数据先缓存，当时间到了一次性计算
         * 2、需要设置4个类型，分别是：输入类型，输出类型，keyBy时key的类型（如果用字符串来划分key类型为Tuple），窗口类型
         * 3、所有的计算都在apply中进行，可以通过window获取窗口的信息，比如开始时间和结束时间
         */
        new WindowFunction[Tuple2[String, Long], String, Tuple, TimeWindow] {
          override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
            val keyStr = key.toString
            // 将window中的数据保存到arrayBuff中
            val arrBuff = ArrayBuffer[Long]()
            input.foreach(tup => {
              arrBuff.append(tup._2)
            })

            // 将arrBuff转换arr
            val arr = arrBuff.toArray
            // 对arr中的数据进行排序
            Sorting.quickSort(arr)

            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            // 将目前Window内排序后的数据，以及Window的开始时间和Window的结束时间打印出来，便于观察
            val result = keyStr + "," + arr.length + "," + sdf.format(arr.head) + "," + sdf.format(arr.last) + "," + sdf.format(window.getStart) + "," + sdf.format(window.getEnd)
            println(result)
          }
        }).print()

    env.execute(this.getClass.getSimpleName)
  }
}
