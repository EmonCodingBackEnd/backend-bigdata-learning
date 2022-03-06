package com.coding.bigdata.flink.stream.transformation;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

/*
 * 使用sideoutput切分流
 */
public class StreamSideOutputJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> text =
                env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        // 按照数据的奇偶性对数据进行分流
        // 首先定义两个sideoutput来准备保存切分出来的数据
        OutputTag<Integer> outputTag1 =
                new OutputTag<Integer>("even") {
                    private static final long serialVersionUID = 6301234458712223123L;
                }; // 保存偶数
        OutputTag<Integer> outputTag2 =
                new OutputTag<Integer>("odd") {
                    private static final long serialVersionUID = -6912215515967612675L;
                }; // 保存奇数

        // 注意：process属于Flink中的低级API
        SingleOutputStreamOperator<Integer> outputStream =
                text.process(
                        new ProcessFunction<Integer, Integer>() {
                            @Override
                            public void processElement(
                                    Integer integer, Context context, Collector<Integer> collector)
                                    throws Exception {
                                if (integer % 2 == 0) {
                                    context.output(outputTag1, integer);
                                } else {
                                    context.output(outputTag2, integer);
                                }
                            }
                        });

        // 获取偶数数据流
        DataStream<Integer> evenStream = outputStream.getSideOutput(outputTag1);
        // 获取偶数数据流
        DataStream<Integer> oddStream = outputStream.getSideOutput(outputTag2);

        evenStream.print().setParallelism(1);

        // 对eventStream流进行二次切分
        OutputTag<Integer> outputTag11 =
                new OutputTag<Integer>("low") {
                    private static final long serialVersionUID = 884047759631737313L;
                }; // 保存小于等于5的数字
        OutputTag<Integer> outputTag22 =
                new OutputTag<Integer>("high") {
                    private static final long serialVersionUID = 4986684498780004383L;
                }; // 保存大于5的数字

        SingleOutputStreamOperator<Integer> subOutputStream =
                evenStream.process(
                        new ProcessFunction<Integer, Integer>() {
                            @Override
                            public void processElement(
                                    Integer integer, Context context, Collector<Integer> collector)
                                    throws Exception {
                                if (integer <= 5) {
                                    context.output(outputTag11, integer);
                                } else {
                                    context.output(outputTag22, integer);
                                }
                            }
                        });

        // 获取小于等于5的数据量
        DataStream<Integer> lowStream = subOutputStream.getSideOutput(outputTag11);
        // 获取大于5的数据量
        DataStream<Integer> highStream = subOutputStream.getSideOutput(outputTag22);

        lowStream.print().setParallelism(1);

        env.execute(StreamSideOutputJava.class.getSimpleName());
    }
}
