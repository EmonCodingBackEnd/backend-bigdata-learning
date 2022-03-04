package com.coding.bigdata.flink.batch;

import com.coding.bigdata.common.EnvUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/*
 * 需求：统计指定文件中单词出现的总次数
 */
public class BatchWordCountJava {
    public static void main(String[] args) throws Exception {
        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        boolean isWinLocal = true;
        String inputPath = "custom/data/flink/batch/input/hello.txt";
        String outputPath = "custom/data/flink/batch/output";
        if (!EnvUtils.isWin || !isWinLocal) {
            isWinLocal = false;
            inputPath = EnvUtils.toHDFSPath(inputPath);
            outputPath = EnvUtils.toHDFSPath(outputPath);
        }
        EnvUtils.checkInputPath(inputPath, isWinLocal);
        EnvUtils.checkOutputPath(outputPath, isWinLocal);

        // 读取文件中的数据
        DataSource<String> text = env.readTextFile(inputPath);

        // 处理数据
        AggregateOperator<Tuple2<String, Integer>> wordCount =
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
                            public Tuple2<String, Integer> map(String word)
                                    throws Exception {
                                return new Tuple2<String, Integer>(word, 1);
                            }
                        })*/
                text.flatMap(
                                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                                    public void flatMap(
                                            String line, Collector<Tuple2<String, Integer>> out)
                                            throws Exception {
                                        String[] words = line.split(" ");
                                        for (String word : words) {
                                            out.collect(new Tuple2<String, Integer>(word, 1));
                                        }
                                    }
                                })
                        .groupBy(0)
                        .sum(1)
                        .setParallelism(1);

        // 使用一个线程执行打印操作
        wordCount.writeAsCsv(outputPath, "\n", " ");

        // 执行程序
        env.execute(BatchWordCountJava.class.getSimpleName());
    }
}
