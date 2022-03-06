package com.coding.bigdata.flink.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/*
 * 需求：通过socket实时产生一些单次
 * 使用Flink实时接收数据
 * 对指定时间窗口内（例如：2秒）的数据进行聚合统计
 * 并且把时间窗口内计算的结果打印出来
 *
 * 前提：在启动该程序之前，先在指定主机emon启动命令： nc -lk 9000
 * 等启动程序后，在emon主机终端输入： hello you hello me
 *
 * 任务提交方式：
 * 方式1、Window环境执行方式：直接在IDEA中执行，方便在本地环境调试代码
 * 方式2、Linux环境执行方式：使用flink提交到集群执行【实际工作中使用】
 * 创建脚本： [emon@emon ~]$ vim /home/emon/bigdata/flink/shell/socketWindowWordCountScala.sh
 * 脚本内容：如下：
flink \
run -m yarn-cluster \
-c com.coding.bigdata.flink.stream.SocketWindowWordCountScala \
-yjm 1024 -ytm 1024 \
/home/emon/bigdata/flink/lib/flink-learning-1.0-SNAPSHOT-jar-with-dependencies.jar
 * 修改脚本可执行权限：[emon@emon ~]$ chmod u+x /home/emon/bigdata/flink/shell/socketWindowWordCountScala.sh
 * 执行脚本：[emon@emon ~]$ sh -x /home/emon/bigdata/flink/shell/socketWindowWordCountScala.sh
 */
object SocketWindowWordCountScala {

  def main(args: Array[String]): Unit = {
    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 连接socket获取输入数据
    val text = env.socketTextStream("emon", 9000)

    // 处理数据
    // 注意：必须要添加这一行隐式转换的代码，否则下面的flatMap方法会报错
    import org.apache.flink.api.scala._
    val wordCount = text.flatMap(_.split(" ")) // 将每一行数据根据空格切分单词
      .map((_, 1)) // 每一个单词转换为tuple2的形式（单词，1）
      // .keyBy(0) // 根据tuple2中的第一列进行分组
      .keyBy(tup => tup._1) // 官方推荐使用keySelector选择器选择数据
      .timeWindow(Time.seconds(2)) // 时间窗口为2秒，表示每隔2秒钟计算一次接收到的数据
      //.sum(1)  // 使用sum或者reduce都可以
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))

    // 使用一个线程执行打印操作
    wordCount.print().setParallelism(1)

    // 执行程序
    env.execute(this.getClass.getSimpleName)

  }
}
