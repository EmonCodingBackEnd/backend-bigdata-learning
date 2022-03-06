package com.coding.bigdata.flink.batch.transformation

import org.apache.flink.api.common.operators.Order

import scala.collection.mutable.ListBuffer

/*
 * first-n：获取集合中的前N个元素
 */
object BatchFirstNScala {

  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = ListBuffer[Tuple2[Int, String]]()
    data.append((2, "zs"))
    data.append((4, "ls"))
    data.append((3, "ww"))
    data.append((1, "aw"))
    data.append((1, "xw"))
    data.append((1, "mw"))

    // 初始化数据
    val text: DataSet[(Int, String)] = env.fromCollection(data)

    // 获取前3条数据，按照数据插入的顺序
    text.first(3).print()

    println("==================================================")

    // 根据数据中的第一列进行分组，获取每组的前2个元素
    text.groupBy(0).first(2).print()

    println("==================================================")

    // 根据数据中的第一列分组，再根据第二列进行组内排序[倒序]，获取每组的前2个元素
    // 跟住排序获取TopN
    text.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print()
  }
}
