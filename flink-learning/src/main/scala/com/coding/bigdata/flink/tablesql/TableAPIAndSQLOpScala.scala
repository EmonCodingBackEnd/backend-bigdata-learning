package com.coding.bigdata.flink.tablesql

import com.coding.bigdata.common.EnvUtils
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}

/*
 * TableAPI和SQL的使用
 */
object TableAPIAndSQLOpScala {

  def main(args: Array[String]): Unit = {
    // 获取TableEnvironment
    val streamSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    // 创建TableEnvironment对象
    val streamTableEnv: TableEnvironment = TableEnvironment.create(streamSettings)

    // 创建输入表
    /*
     * connector.type：指定connector的类型
     * connector.path：指定文件或者目录地址
     * format.type：文件数据格式化类型，现在只支持csv格式
     */
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

    // 使用TableAPI实现数据查询和过滤等操作
    /*import org.apache.flink.table.api._
    val result: Table = streamTableEnv.from("myTable")
      .select($"id", $"name")
      .filter($"id" > 1)*/

    // 使用SQL实现数据查询和过滤等操作
    val result: Table = streamTableEnv.sqlQuery("select id,name from myTable where id > 1")

    // 输出结果到控制台
    result.execute().print()

    // 创建输出表
    streamTableEnv.executeSql(
      """
        |create table newTable(
        |id int,
        |name string
        |) with (
        |'connector.type' = 'filesystem',
        |'connector.path' = 'custom/data/flink/source/output',
        |'format.type' = 'csv'
        |)
        |""".stripMargin)

    // 输出结果到表newTable中
    EnvUtils.checkOutputPath("custom/data/flink/source/output", true)
    result.executeInsert("newTable")
  }
}
