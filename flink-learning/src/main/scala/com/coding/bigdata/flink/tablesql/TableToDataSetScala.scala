package com.coding.bigdata.flink.tablesql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.flink.types.Row

/*
 * 将table转换成DataSet
 */
object TableToDataSetScala {

  def main(args: Array[String]): Unit = {
    // 获取 BatchTableEnvironment
    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    // 注意：此时只能使用旧的执行引擎，新的Blink执行引擎不支持和DataSet转换
    val batchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)


    // 创建输入表
    batchTableEnv.executeSql(
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
    val table: Table = batchTableEnv.from("myTable")

    // 将Table转换成DataSet
    import org.apache.flink.api.scala._
    val set = batchTableEnv.toDataSet[Row](table)
    set.map(row => (row.getField(0).toString.toInt, row.getField(1).toString)).print()
  }
}
