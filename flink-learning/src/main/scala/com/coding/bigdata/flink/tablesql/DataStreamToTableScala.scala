package com.coding.bigdata.flink.tablesql

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/*
 * 将DataStream转换为表
 */
object DataStreamToTableScala {
  def main(args: Array[String]): Unit = {
    // 获取 StreamTableEnvironment
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val streamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, streamSettings)

    // 获取DataStream
    import org.apache.flink.api.scala._
    val stream: DataStream[(Int, String)] = streamEnv.fromCollection(Array((1, "jack"), (2, "tom"), (3, "mack")))

    // 第一种：将DataStream转换为view视图
    import org.apache.flink.table.api._
    streamTableEnv.createTemporaryView("myTable", stream, 'id, 'name)
    streamTableEnv.sqlQuery("select * from myTable where id > 1").execute().print()

    // 第二种：将DataStream转换为Table对象
    val table: Table = streamTableEnv.fromDataStream(stream, $"id", $"name")
    table.select($"id", $"name")
      .filter($"id" > 1)
      .execute()
      .print()

    // 注意：'id,'name 和 $"id",$"name" 这两种写法是一样的效果。

  }
}
