package com.coding.bigdata.flink.tablesql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/*
 * 创建TableEnvironment对象
 */
object CreateTableEnvironmentScala {

  def main(args: Array[String]): Unit = {
    /*
     * 注意：如果Table API和SQL不需要和DataStream或者DataSet互相转换
     * 则针对stream和batch都可以使用TableEnvironment
     */
    // 指定底层引擎为Blink，以及数据处理模式-stream
    val streamSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    // 创建TableEnvironment对象
    val streamTableEnv: TableEnvironment = TableEnvironment.create(streamSettings)


    // 指定底层引擎为Blink，以及数据处理模式-batch
    val batchSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    // 创建TableEnvironment对象
    val batchTableEnv: TableEnvironment = TableEnvironment.create(batchSettings)


    /*
     * 注意：如果Table API和SQL需要和DataStream或者DataSet互相转换
     * 则针对stream需要使用 StreamTableEnvironment
     * 针对batch需要使用 BatchTableEnvironment
     */
    // 创建 StreamTableEnvironment
    val streamEnv2 = StreamExecutionEnvironment.getExecutionEnvironment
    val streamSettings2 = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv2: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv2, streamSettings2)


    // 创建 BatchTableEnvironment
    val batchEnv2 = ExecutionEnvironment.getExecutionEnvironment
    // 注意：此时只能使用旧的执行引擎，新的Blink执行引擎不支持和DataSet转换
    val batchTableEnv2: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv2)
  }
}
