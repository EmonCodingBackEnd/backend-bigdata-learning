package com.coding.bigdata.flink.stream.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/*
 * 分区规则的使用。
 */
public class StreamPartitionOpJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 注意：默认情况下Flink任务中算子的并行度会读取当前机器的CPU个数
        // fromCollection的并行度为1
        DataStreamSource<Integer> text =
                env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        // 使用shuffle分区规则
        // shuffleOp(text);

        // 使用rebalance分区规则
        // rebalanceOp(text);

        // 使用rescale分区规则
        // rescaleOp(text);

        // 使用broadcast分区规则
        // broadcastOp(text);

        // 自定义分区规则：根据数据的奇偶性进行分区
        customPartitionOp(text);

        env.execute(StreamPartitionOpJava.class.getSimpleName());
    }

    // 广播每一个元素到下游的每一个分区
    private static void customPartitionOp(DataStreamSource<Integer> text) {
        text.map(
                        new MapFunction<Integer, Integer>() {
                            @Override
                            public Integer map(Integer value) throws Exception {
                                return value;
                            }
                        })
                .setParallelism(2)
                // .partitionCustom(new MyPartitionerJava(), 0) // 这种写法已经过时了
                .partitionCustom(
                        new MyPartitionerJava(),
                        new KeySelector<Integer, Integer>() {
                            @Override
                            public Integer getKey(Integer key) throws Exception {
                                return key;
                            }
                        }) // 官方建议使用keySelector
                .print()
                .setParallelism(4);
    }

    // 广播每一个元素到下游的每一个分区
    private static void broadcastOp(DataStreamSource<Integer> text) {
        text.map(
                        new MapFunction<Integer, Integer>() {
                            @Override
                            public Integer map(Integer value) throws Exception {
                                return value;
                            }
                        })
                .setParallelism(2)
                .broadcast()
                .print()
                .setParallelism(4);
    }

    /*
     * 如果上游操作有2个并发，而下游操作有4个并发，那么上游的1个并发结果循环分配给下游的2个并发操作，
     * 上游的另外1个并发结果循环分配到下游的另外2个并发操作。
     * 另一种情况，如果上游有4个并发操作，而下游有2个并发操作，那么上游的其中2个并发的结果会分配给下游的一个并发操作，
     * 而上游的另外2个并发操作的结果则分配给下游的另外1个并发操作。
     * 注意：rescale与rebalance的区别是reblance会产生全量重分区，而rescale不会。
     */
    private static void rescaleOp(DataStreamSource<Integer> text) {
        text.map(
                        new MapFunction<Integer, Integer>() {
                            @Override
                            public Integer map(Integer value) throws Exception {
                                return value;
                            }
                        })
                .setParallelism(2)
                .rescale()
                .print()
                .setParallelism(4);
    }

    // 循环分区
    private static void rebalanceOp(DataStreamSource<Integer> text) {
        text.map(
                        new MapFunction<Integer, Integer>() {
                            @Override
                            public Integer map(Integer value) throws Exception {
                                return value;
                            }
                        })
                .setParallelism(2)
                .rebalance()
                .print()
                .setParallelism(4);
    }

    // 随机分区
    private static void shuffleOp(DataStreamSource<Integer> text) {
        text.map(
                        new MapFunction<Integer, Integer>() {
                            @Override
                            public Integer map(Integer value) throws Exception {
                                return value;
                            }
                        })
                .setParallelism(2)
                .shuffle()
                .print()
                .setParallelism(4);
    }
}
