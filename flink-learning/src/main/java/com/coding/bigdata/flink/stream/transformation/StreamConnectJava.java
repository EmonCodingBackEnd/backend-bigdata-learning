package com.coding.bigdata.flink.stream.transformation;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/*
 * 只能连接两个流，两个流的数据类型可以不同
 * 应用：可以将两种不同格式的数据统一成一种格式
 */
public class StreamConnectJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 第一份数据
        DataStreamSource<String> text1 = env.fromElements("user:tom,age:18");

        // 第二份数据
        DataStreamSource<String> text2 = env.fromElements("user:jack_age:20");

        // 连接两个流
        ConnectedStreams<String, String> connectedStreams = text1.connect(text2);

        connectedStreams
                .map(
                        new CoMapFunction<String, String, String>() {
                            @Override
                            public String map1(String s) throws Exception {
                                return s.replace(",", "-");
                            }

                            @Override
                            public String map2(String s) throws Exception {
                                return s.replace("_", "-");
                            }
                        })
                .print()
                .setParallelism(1);

        env.execute(StreamConnectJava.class.getSimpleName());
    }
}
