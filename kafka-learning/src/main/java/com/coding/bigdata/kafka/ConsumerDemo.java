package com.coding.bigdata.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

/** 需求：使用Java代码实现消费者代码 */
public class ConsumerDemo {
    public static void main(String[] args) {
        Properties prop = new Properties();
        // 指定Kafka的broker地址
        prop.put("bootstrap.servers", "emon:9092,emon2:9092,emon3:9092");
        // 指定key-value数据的序列化格式
        prop.put("key.deserializer", StringDeserializer.class.getName());
        prop.put("value.deserializer", StringDeserializer.class.getName());

        // 指定消费者组
        prop.put("group.id", "con-2");

        // 默认true，开启自动提交offset功能
        prop.put("enable.auto.commit", true);
        // 自动提交offset的时间间隔，单位是毫秒
        prop.put("auto.commit.interval.ms", "5000");
        /*
         * 注意：正常情况下，Kafka消费数据的流程是这样的：
         * 先根据group.id指定的消费者组到Kafka中查找之前保存的offset信息，
         * 如果查找到了，说明之前使用这个消费者组消费过数据，则根据之前保存的offset继续进行消费，
         * 如果没有查找到（说明第一次消费），或者查找到了，但是查找到的那个offset对应的数据已经不存在了，
         * 这个时候消费者该如何消费数据？
         * （因为Kafka默认只会保存7天的数据，超过时间数据会被删除）
         *
         * 此时会根据auto.offset.reset的值执行不同的消费逻辑。
         * 这个参数的值有三种：【earliest，latest，none】
         *
         * earliest：表示从最早的数据开始消费（从头消费）
         * latest【默认】：表示从最新的数据开始消费
         * none：如果根据指定的group.id没有找到之前消费的offset信息，就会抛出异常。
         *
         * 解释：【查找到了，但是查找到的那个offset对应的数据已经不存在了】
         */
        prop.put("auto.offset.reset", "earliest"); // 如果调整该值，还需要调整更换消费者组，因为原消费者组可能被提交状态了

        // 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        Collection<String> topics = new ArrayList<>();
        topics.add("hello");

        // 订阅指定的topic
        consumer.subscribe(topics);

        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : poll) {
                System.out.println(consumerRecord);
            }
        }
    }
}
