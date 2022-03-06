package com.coding.bigdata.flink.tablesql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment

/*
 * 将DataSet转换为表
 */
object DataSetToTableScala {

  def main(args: Array[String]): Unit = {
    // 获取 BatchTableEnvironment
    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    // 注意：此时只能使用旧的执行引擎，新的Blink执行引擎不支持和DataSet转换
    val batchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)

    // 获取DataSet
    import org.apache.flink.api.scala._
    val set: DataSet[(Int, String)] = batchEnv.fromCollection(Array((1, "jack"), (2, "tom"), (3, "mack")))

    // 第一种：将DataSet转换为view视图
    import org.apache.flink.table.api._
    batchTableEnv.createTemporaryView("myTable", set, 'id, 'name)
    batchTableEnv.sqlQuery("select * from myTable where id > 1").execute().print()

    // 第二种：将DataSet转换为table对象1
    val table: Table = batchTableEnv.fromDataSet(set, 'id, 'name)
    table.select($"id", $"name")
      .filter($"id" > 1)
      .execute()
      .print()

  }
}
