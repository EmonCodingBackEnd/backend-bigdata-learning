package com.coding.bigdata.flink.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/*
 * 需求：自定义MyWindow的使用
 * 1：滚动窗口
 *
 * 前提：在启动该程序之前，先在指定主机emon启动命令： nc -lk 9000
 * 等启动程序后，在emon主机终端输入： hello you hello me
 */
public class MyTimeWindowOpJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("emon", 9000);

        // 自定义MyTimeWindow滚动窗口：每隔10秒计算一次前10秒时间窗口内的数据
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
                // 窗口大小
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1)
                .print();

        // TimeWindow之滑动窗口：每隔5秒计算一次前10秒时间窗口内的数据
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
        // 第一个参数：窗口大小；第二个参数：滑动间隔
        .timeWindow(Time.seconds(10), Time.seconds(5))
        .sum(1)
        .print();*/

        // 执行程序
        env.execute(MyTimeWindowOpJava.class.getSimpleName());
    }
}
