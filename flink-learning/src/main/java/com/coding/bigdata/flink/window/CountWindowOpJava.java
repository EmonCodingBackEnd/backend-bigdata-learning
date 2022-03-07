package com.coding.bigdata.flink.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
 * CountWindow的使用
 * 1：滚动窗口
 * 2：滑动窗口
 *
 * 前提：在启动该程序之前，先在指定主机emon启动命令： nc -lk 9000
 * 等启动程序后，在emon主机终端输入： hello you hello me
 */
public class CountWindowOpJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("emon", 9000);

        // CountWindow之滚动窗口：每隔5个元素计算一次前5个元素：非常注意，由于是分组了，表示分组内每隔元素的次数达到5个才会输出一次
        /*text.flatMap(
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
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                })
        .keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> tup) throws Exception {
                        return tup.f0;
                    }
                })
        // 窗口大小
        .countWindow(5)
        .sum(1)
        .print();*/

        // CountWindow之滑动窗口：每隔1个元素计算一次前5个元素
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
                            public Tuple2<String, Integer> map(String word) throws Exception {
                                return new Tuple2<String, Integer>(word, 1);
                            }
                        })
                .keyBy(
                        new KeySelector<Tuple2<String, Integer>, String>() {
                            @Override
                            public String getKey(Tuple2<String, Integer> tup) throws Exception {
                                return tup.f0;
                            }
                        })
                // 第一个参数：窗口大小；第二个参数：滑动间隔
                .countWindow(5, 1)
                .sum(1)
                .print();

        // 执行程序
        env.execute(CountWindowOpJava.class.getSimpleName());
    }
}
