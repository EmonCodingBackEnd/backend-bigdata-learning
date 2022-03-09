package com.coding.bigdata.flink.kafkaconnector

import com.coding.bigdata.common.EnvUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

/*
 * Flink从Kafka中消费数据
 *
 * 环境准备：
 * 1、启动zookeeper和kafka服务
 * 2、创建topic
 * kafka-topics.sh --create --zookeeper emon:2181 --partitions 2 --replication-factor 1 --topic flinktest1
 * 3、启动本应用程序
 * 4、控制台打开生产者
 * kafka-console-producer.sh --broker-list emon:9092 --topic flinktest1
 * 5、启用Checkpoint时，还需要依赖Hadoop的hdfs。
 * start-all.sh
 * 6、在Kafka生产者命令行
 * 在命令行窗口输入数据，比如：hello flink
 */
object StreamKafkaSourceScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 每隔5000ms执行一次Checkpoint（设置Checkpoint的周期）
    env.enableCheckpointing(5000)

    // 针对Checkpoint的相关配置
    // 设置模式为：EXACTLY_ONCE（这是默认值），还可以设置为AT_LEAST_ONCE
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确保两次Checkpoint之间有至少多少ms的间隔（Checkpoint最小间隔）
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // Checkpoint必须在一分钟内完成，或者被丢弃（Checkpoint的超时时间）
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 同一时间只允许执行一个Checkpoint
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val checkpointPath = "hdfs://emon:8020/checkpoint/flink/chk001"
    // 清理checkpoint目录：非必须；但在win环境执行该方法，可避免访问hdfs权限问题
    EnvUtils.checkOutputPath(checkpointPath, false)
    // 设置状态数据存储的位置
    // env.setStateBackend(new RocksDBStateBackend(checkpointPath, true)) // 这种方式提示代码优化为下面一行
    env setStateBackend new RocksDBStateBackend(checkpointPath, true)


    // 指定 FlinkKafkaConsumer 相关配置
    val topic = "flinktest1"
    val prop = new Properties();
    // 指定Kafka的broker地址
    prop.put("bootstrap.servers", "emon:9092")
    // 指定消费者组
    prop.put("group.id", "flinkcon1")

    val kafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop)

    // KafkaConsumer的消费策略设置
    // 默认策略：读取group.id对应保存的offset开始消费数据，读取不到则根据kafka中auto.offset.reset参数的值开始消费数据
    kafkaConsumer.setStartFromGroupOffsets()
    // 从最早的记录开始消费数据，忽略已提交的offset信息
    //    kafkaConsumer.setStartFromEarliest()
    // 从最新的记录开始消费数据，忽略已提交的offset信息
    //    kafkaConsumer.setStartFromLatest()
    // 从指定的时间戳开始消费记录，对于每个分区，其时间戳大于或等于指定时间戳的记录，将被作为起始位置
    //    kafkaConsumer.setStartFromTimestamp()

    // 指定Kafka作为source
    import org.apache.flink.api.scala._
    val text: DataStream[String] = env.addSource(kafkaConsumer)

    // 将读取到的数据输出
    text.print()

    env.execute(this.getClass.getSimpleName)
  }
}
