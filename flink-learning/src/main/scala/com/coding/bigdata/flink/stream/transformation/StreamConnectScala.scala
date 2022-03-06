package com.coding.bigdata.flink.stream.transformation

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, StreamExecutionEnvironment}

/*
 * 只能连接两个流，两个流的数据类型可以不同
 * 应用：可以将两种不同格式的数据统一成一种格式
 */
object StreamConnectScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    // 第一份数据
    val text1 = env.fromElements("user:tom,age:18")
    // 第二份数据
    val text2 = env.fromElements("user:jack_age:20")


    // 连接两个流
    val connectedStreams: ConnectedStreams[String, String] = text1.connect(text2)

    connectedStreams.map(new CoMapFunction[String, String, String] {
      // 处理第一份数据流中的数据
      override def map1(in1: String): String = {
        in1.replace(",", "-")
      }

      // 处理第二份数据流中的数据
      override def map2(in2: String): String = {
        in2.replace("_", "-")
      }
    }).print().setParallelism(1)

    env.execute(this.getClass.getSimpleName)
  }
}
