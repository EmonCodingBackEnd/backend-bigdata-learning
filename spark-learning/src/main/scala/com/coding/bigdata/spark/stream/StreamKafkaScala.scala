package com.coding.bigdata.spark.stream

import com.coding.bigdata.common.EnvScalaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
 * Spark消费Kafka中的数据
 *
 * 环境准备：
 * 1、启动zookeeper和kafka服务
 * 2、创建topic
 * kafka-topics.sh --create --zookeeper emon:2181 --partitions 2 --replication-factor 1 --topic sparktest1
 * 3、启动本应用程序
 * 4、控制台打开生产者
 * kafka-console-producer.sh --broker-list emon:9092 --topic sparktest1
 * 5、在Kafka生产者命令行
 * 在命令行窗口输入数据，比如：hello flink
 */
object StreamKafkaScala {

  def main(args: Array[String]): Unit = {
    // 第一步：创建SparkConf
    val conf = EnvScalaUtils.buildSparkConfByEnv(this.getClass.getSimpleName)

    // 创建StreamingContext，指定数据处理间隔为5秒
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    // 指定Kafka的配置信息
    val kafkaParams = Map[String, Object](
      // Kafka的broker地址信息；多个英文逗号分隔，比如： emon:9092,emon2:9092,emon3:9092
      "bootstrap.servers" -> "emon:9092",
      // key的序列化类型
      "key.deserializer" -> classOf[StringDeserializer],
      // value的序列化类型
      "value.deserializer" -> classOf[StringDeserializer],
      // 消费者组ID
      "group.id" -> "sparkcon1",
      // 消费策略 latest 、 earliest
      "auto.offset.reset" -> "latest",
      // 自动提交offset；同时，为了解决问题 the result type of an implicit conversion must be more specific than Object 不能直接赋值 true
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    // 指定要读取的topic名称
    val topics = Array("sparktest1")

    // 获取消费Kafka的数据量
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // 处理数据
    kafkaDStream.map(record => (record.key(), record.value()))
      // 将数据打印到控制台
      .print()

    // 启动任务
    ssc.start()

    // 等待任务停止
    ssc.awaitTermination()
  }
}
