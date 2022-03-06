package com.coding.bigdata.flink.stream.transformation

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
 * 合并多个流，多个流的数据类型必须一致
 * 应用场景：多个数据源的数据类型一致，数据处理规则也一致
 */
object StreamUnionScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    // 第一份数据
    val text1 = env.fromCollection(Array(1, 2, 3, 4, 5))
    // 第二份数据
    val text2 = env.fromCollection(Array(6, 7, 8, 9, 10))

    val unionStream: DataStream[Int] = text1.union(text2)

    unionStream.print().setParallelism(1)

    env.execute(this.getClass.getSimpleName)
  }
}
