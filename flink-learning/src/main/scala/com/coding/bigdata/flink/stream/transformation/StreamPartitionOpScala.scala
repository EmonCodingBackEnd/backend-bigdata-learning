package com.coding.bigdata.flink.stream.transformation

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
 * 分区规则的使用。
 */
object StreamPartitionOpScala {

  // 注意：在这里将这个隐式转换代码放到类上面，可以对整个对象中的main方法和shuffleOp方法都生效

  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    // 注意：默认情况下Flink任务中算子的并行度会读取当前机器的CPU个数
    // fromCollection的并行度为1
    val text = env.fromCollection(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // 使用shuffle分区规则
    // shuffleOp(text)

    // 使用rebalance分区规则
    // rebalanceOp(text)

    // 使用rescale分区规则
    // rescaleOp(text)

    // 使用broadcast分区规则
    // broadcastOp(text)

    // 自定义分区规则：根据数据的奇偶性进行分区
    customPartitionOp(text)


    env.execute(this.getClass.getSimpleName)
  }

  private def customPartitionOp(text: DataStream[Int]) = {
    // 注意：此时虽然print算子的并行度为4，但是自定义的分区规则只会把数据分区分发给2个并行度，所以有2个是不干活的
    text.map(num => num)
      .setParallelism(2) // 设置map算子的并行度为2
      // .partitionCustom(new MyPartitionerScala, 0) // 这种写法已经过时了
      .partitionCustom(new MyPartitionerScala, num => num) // 官方建议使用keySelector
      .print()
      .setParallelism(4) // 设置print算子的并行度为4
  }

  // 广播每一个元素到下游的每一个分区
  private def broadcastOp(text: DataStream[Int]) = {
    text.map(num => num)
      .setParallelism(2) // 设置map算子的并行度为2
      .broadcast
      .print()
      .setParallelism(4) // 设置print算子的并行度为4
  }

  /*
   * 如果上游操作有2个并发，而下游操作有4个并发，那么上游的1个并发结果循环分配给下游的2个并发操作，
   * 上游的另外1个并发结果循环分配到下游的另外2个并发操作。
   * 另一种情况，如果上游有4个并发操作，而下游有2个并发操作，那么上游的其中2个并发的结果会分配给下游的一个并发操作，
   * 而上游的另外2个并发操作的结果则分配给下游的另外1个并发操作。
   * 注意：rescale与rebalance的区别是reblance会产生全量重分区，而rescale不会。
   */
  private def rescaleOp(text: DataStream[Int]) = {
    text.map(num => num)
      .setParallelism(2) // 设置map算子的并行度为2
      .rescale
      .print()
      .setParallelism(4) // 设置print算子的并行度为4
  }

  // 循环分区
  private def rebalanceOp(text: DataStream[Int]) = {
    text.map(num => num)
      .setParallelism(2) // 设置map算子的并行度为2
      .rebalance
      .print()
      .setParallelism(4) // 设置print算子的并行度为4
  }

  // 随机分区
  private def shuffleOp(text: DataStream[Int]) = {
    // 由于fromCollection已经设置了并行度1，所以需要再接一个算子才能修改并行度，在这使用map算子
    text.map(num => num)
      .setParallelism(2) // 设置map算子的并行度为2
      .shuffle
      .print()
      .setParallelism(4) // 设置print算子的并行度为4
  }
}
