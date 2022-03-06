package com.coding.bigdata.flink.stream.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import scala.Tuple2;

/*
 * 需求：接收Socket传输过来的数据，把数据保存到Redis的List队列中
 *
 * 前提：在启动该程序之前，先在指定主机emon启动命令： nc -lk 9000
 * 等启动程序后，在emon主机终端输入： hello you hello me
 */
public class StreamRedisSinkJava {

    public static void main(String[] args) throws Exception {
        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 连接socket获取输入数据
        DataStreamSource<String> text = env.socketTextStream("emon", 9000);

        // 组装数据，这里组装的是tuple2类型
        // 第一个元素是指list队列的key名称
        // 第二个元素是指需要向list队列中添加的元素
        SingleOutputStreamOperator<Tuple2<String, String>> listData =
                text.map(
                        new MapFunction<String, Tuple2<String, String>>() {
                            @Override
                            public Tuple2<String, String> map(String word) throws Exception {
                                return new Tuple2<>("l_words_scala", word);
                            }
                        });

        // 指定redisSink
        FlinkJedisPoolConfig conf =
                new FlinkJedisPoolConfig.Builder()
                        .setHost("emon")
                        .setPort(6379)
                        .setPassword("redis123")
                        .build();
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(conf, new MyRedisMapper());
        listData.addSink(redisSink);

        // 执行程序
        env.execute(StreamRedisSinkJava.class.getSimpleName());
    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {
        private static final long serialVersionUID = 6504831188432392500L;

        // 指定具体的操作命令
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }
        // 获取key
        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data._1;
        }

        // 获取value

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data._2;
        }
    }
}
