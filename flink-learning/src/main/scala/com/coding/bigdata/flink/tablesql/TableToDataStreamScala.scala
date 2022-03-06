package com.coding.bigdata.flink.tablesql

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row

/*
 * 将table转换为DataStream1
 */
object TableToDataStreamScala {

  def main(args: Array[String]): Unit = {
    // 获取 StreamTableEnvironment
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val streamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, streamSettings)


    // 创建输入表
    streamTableEnv.executeSql(
      """
        |create table myTable(
        |id int,
        |name string
        |) with (
        |'connector.type' = 'filesystem',
        |'connector.path' = 'custom/data/flink/source/input',
        |'format.type' = 'csv'
        |)
        |""".stripMargin)

    // 获取table
    val table: Table = streamTableEnv.from("myTable")

    // 将table转换为DataStream
    // 如果只有新增（追加）操作，可以使用toAppendStream
    import org.apache.flink.api.scala._
    val appStream: DataStream[Row] = streamTableEnv.toAppendStream[Row](table)
    appStream.map(row => (row.getField(0).toString.toInt, row.getField(1).toString)).print()

    // 如果有增加操作，还有删除操作，则使用toRetractStream
    val retStream = streamTableEnv.toRetractStream[Row](table)
    retStream.map(tup => {
      val flag = tup._1
      val row = tup._2
      val id = row.getField(0).toString.toInt
      val name = row.getField(1).toString
      (flag, id, name)
    }).print()

    // 注意：将table转换为DataStream之后，就需要调用StreamExecutionEnvironment中的execute方法了
    streamEnv.execute(this.getClass.getSimpleName)
  }
}
