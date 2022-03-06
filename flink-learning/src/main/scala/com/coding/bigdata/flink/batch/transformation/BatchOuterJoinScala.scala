package com.coding.bigdata.flink.batch.transformation

/*
 * outerJoin：外连接
 * 一共有三种情况
 * 1：leftOuterJoin
 * 2：rightOuterJoin
 * 3：fullOuterJoin
 */
object BatchOuterJoinScala {

  import org.apache.flink.api.scala._


  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 初始化第一份数据 Tuple2<用户id, 用户姓名>
    val text1: DataSet[(Int, String)] = env.fromCollection(Array((1, "jack"), (2, "tom"), (3, "mick")))
    // 初始化第二份数据 Tuple2<用户id, 用户所在城市>
    val text2: DataSet[(Int, String)] = env.fromCollection(Array((1, "bj"), (2, "sh"), (4, "gz")))

    // 对两份数据集执行 leftOuterJoin 操作
    text1.leftOuterJoin(text2)
      .where(0)
      .equalTo(0) {
        (first, second) => {
          // 注意：second中的元素可能为null
          if (second == null) {
            (first._1, first._2, "null")
          } else {
            (first._1, first._2, second._2)
          }
        }
      }.print()

    println("==================================================")

    // 对两份数据集执行 rightOuterJoin 操作
    text1.rightOuterJoin(text2)
      .where(0)
      .equalTo(0) {
        (first, second) => {
          // 注意：first中的元素可能为null
          if (first == null) {
            (second._1, "null", second._2)
          } else {
            (first._1, first._2, second._2)
          }
        }
      }.print()

    println("==================================================")

    // 对两份数据集执行 fullOuterJoin 操作
    text1.fullOuterJoin(text2)
      .where(0)
      .equalTo(0) {
        (first, second) => {
          // 注意：first中的元素可能为null
          if (first == null) {
            (second._1, "null", second._2)
          }
          // 注意：second中的元素可能为null
          else if (second == null) {
            (first._1, first._2, "null")
          } else {
            (first._1, first._2, second._2)
          }
        }
      }.print()
  }
}
