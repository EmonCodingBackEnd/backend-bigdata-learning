package com.coding.bigdata.flink.batch.transformation

import scala.collection.mutable.ListBuffer

/*
 * MapPartition的使用：一次处理一个分区的数据
 */
object BatchMapPartitionScala {

  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 生成数据源数据
    val text: DataSet[String] = env.fromCollection(Array("hello you", "hello me"))

    // 每次处理一个分区的数据
    text.mapPartition(it => {
      // 可以在此处创建数据库连接，建议把这块代码放到try-cache代码中
      // 注意：此时是每个分区获取一次数据库连接，不需要每处理一条数据就获取一次连接，性能较高
      val res = ListBuffer[String]()
      it.foreach(line => {
        val words = line.split(" ")
        words.foreach(word => {
          res.append(word)
        })
      })
      // 关闭数据库连接
      res
    }).print()

    // No new data sinks have been defined since the last execution. The last execution refers to the latest call to 'execute()', 'count()', 'collect()', or 'print()'.
    // 注意：针对DataSetAPI，如果在后面调用的是count、collect、print，则最后不需要指定execute即可。
    // env.execute(this.getClass.getSimpleName)
  }
}
