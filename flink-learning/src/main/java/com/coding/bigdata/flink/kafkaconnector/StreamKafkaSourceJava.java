package com.coding.bigdata.flink.kafkaconnector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

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
 */
public class StreamKafkaSourceJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 指定FlinkKafkaConsumer相关配置
        String topic = "flinktest1";
        Properties prop = new Properties();
        // 指定Kafka的broker地址
        prop.put("bootstrap.servers", "emon:9092");
        // 指定消费者组
        prop.put("group.id", "flinkcon1");

        FlinkKafkaConsumer<String> kafkaConsumer =
                new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), prop);

        // 指定Kafka作为source
        DataStreamSource<String> text = env.addSource(kafkaConsumer);

        // 将读取到的数据输出
        text.print();

        env.execute(StreamKafkaSourceJava.class.getSimpleName());
    }
}
