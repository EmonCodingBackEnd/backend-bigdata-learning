package com.coding.bigdata.spark.stream;

import com.coding.bigdata.common.EnvUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;

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
public class StreamKafkaJava {

    public static void main(String[] args) throws InterruptedException {
        // 第一步：创建SparkConf
        SparkConf conf = EnvUtils.buildSparkConfByEnv(StreamWordCountJava.class.getSimpleName());

        // 创建StreamingContext，指定数据处理间隔为5秒
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 指定kafka的配置信息
        HashMap<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "emon:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("group.id", "sparkcon1");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);

        // 指定要读取的topic名称
        ArrayList<String> topics = new ArrayList<String>();
        topics.add("sparktest1");

        // 获取消费kafka的数据流
        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        // 处理数据
        kafkaStream
                .map(
                        new Function<ConsumerRecord<String, String>, Tuple2<String, String>>() {
                            public Tuple2<String, String> call(
                                    ConsumerRecord<String, String> record) throws Exception {
                                return new Tuple2<String, String>(record.key(), record.value());
                            }
                        })
                .print(); // 将数据打印到控制台

        // 启动任务
        ssc.start();

        // 等待任务停止
        ssc.awaitTermination();
    }
}
