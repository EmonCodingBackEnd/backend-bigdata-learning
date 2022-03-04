package com.coding.bigdata.flink.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/*
 * 需求：通过socket实时产生一些单次
 * 使用Flink实时接收数据
 * 对指定时间窗口内（例如：2秒）的数据进行聚合统计
 * 并且把时间窗口内计算的结果打印出来
 */
public class SocketWindowWordCountJava {

    public static void main(String[] args) throws Exception {
        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 连接socket获取输入数据
        DataStreamSource<String> text = env.socketTextStream("emon", 9000);

        // 处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount =
                text.flatMap(
                                new FlatMapFunction<String, String>() {
                                    @Override
                                    public void flatMap(String line, Collector<String> collector)
                                            throws Exception {
                                        String[] words = line.split(" ");
                                        for (String word : words) {
                                            collector.collect(word);
                                        }
                                    }
                                })
                        .map(
                                new MapFunction<String, Tuple2<String, Integer>>() {
                                    @Override
                                    public Tuple2<String, Integer> map(String word)
                                            throws Exception {
                                        return new Tuple2<String, Integer>(word, 1);
                                    }
                                })
                        // .keyBy(0)
                        .keyBy(
                                new KeySelector<Tuple2<String, Integer>, String>() {
                                    @Override
                                    public String getKey(Tuple2<String, Integer> tup)
                                            throws Exception {
                                        return tup.f0;
                                    }
                                })
                        .timeWindow(Time.seconds(2))
                        .sum(1); // 使用sum或者reduce都可以

        // 使用一个线程执行打印操作
        wordCount.print().setParallelism(1);

        // 执行程序
        env.execute(SocketWindowWordCountJava.class.getSimpleName());
    }
}
