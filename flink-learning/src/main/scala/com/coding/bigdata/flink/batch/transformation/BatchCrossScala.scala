package com.coding.bigdata.flink.batch.transformation

/*
 * cross：获取两个数据集的笛卡尔积
 */
object BatchCrossScala {

  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 初始化第一份数据
    val text1: DataSet[Int] = env.fromCollection(Array(1, 2))
    // 初始化第二份数据
    val text2: DataSet[String] = env.fromCollection(Array("a", "b"))

    // 执行cross操作
    text1.cross(text2).print()

  }
}
