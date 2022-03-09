package com.coding.bigdata.flink.kafkaconnector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaSerializationSchemaWrapper;

import java.util.Properties;

/*
 * Flink向Kafka中生产数据
 *
 * 环境准备：
 * 1、启动zookeeper和kafka服务
 * 2、创建topic
 * kafka-topics.sh --create --zookeeper emon:2181 --partitions 2 --replication-factor 1 --topic flinktest2
 * 3、在emon机器打开socket
 * nc -lk 9000
 * 4、启动本应用程序
 * 5、控制台打开消费者
 * kafka-console-consumer.sh --bootstrap-server emon:9092 --topic flinktest2 --from-beginning
 * 6、在nc -lk 9000命令行
 * 在命令行窗口输入数据，比如：hello flink
 */
public class StreamKafkaSinkJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("emon", 9000);

        // 指定 FlinkKafkaProducer 相关配置
        String topic = "flinktest2";
        Properties prop = new Properties();
        // 指定Kafka的broker地址
        prop.put("bootstrap.servers", "emon:9092");

        /*
         * KafkaSerializationSchemaWrapper的几个参数：
         * 1、topic：指定需要写入的topic名称即可
         * 2、partitioner：通过自定义分区器实现将数据写入到指定topic的具体分区中
         * 默认会使用 FlinkFixedPartitioner，它表示会将某一个并行度对应的所有的数据都写入指定topic的一个分区里面
         * 如果不想自定义分区器，也不想使用默认的，可以直接使用null即可
         * 3、writeTimeStamp，向topic中写入数据的时候，是否写入时间戳
         * 如果写入了，那么在Watermark的案例中，使用 extractTimestamp() 抽取时间戳的时候，
         * 就可以直接使用 recordTimestamp 即可，它表示的就是我们在这里写入的数据对应的timestamp
         */
        FlinkKafkaProducer<String> kafkaProducer =
                new FlinkKafkaProducer<String>(
                        topic,
                        new KafkaSerializationSchemaWrapper<String>(
                                topic, /*new FlinkFixedPartitioner[String]()*/
                                null,
                                false,
                                new SimpleStringSchema()),
                        prop,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        // 指定Kafka作为sink
        text.addSink(kafkaProducer);

        env.execute(StreamKafkaSinkJava.class.getSimpleName());
    }
}
