package com.coding.bigdata.flink.stream.transformation;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/*
 * 合并多个流，多个流的数据类型必须一致
 * 应用场景：多个数据源的数据类型一致，数据处理规则也一致
 */
public class StreamUnionJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 第一份数据
        DataStreamSource<Integer> text1 = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));

        // 第二份数据
        DataStreamSource<Integer> text2 = env.fromCollection(Arrays.asList(6, 7, 8, 9, 10));

        DataStream<Integer> unionStream = text1.union(text2);

        unionStream.print().setParallelism(1);

        env.execute(StreamUnionJava.class.getSimpleName());
    }
}
