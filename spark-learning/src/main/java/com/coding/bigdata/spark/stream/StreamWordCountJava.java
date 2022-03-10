package com.coding.bigdata.spark.stream;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/*
 * 需求：通过socket模拟产生数据，实时计算数据中单词出现的次数。
 * 环境准备：
 * 1、在emon机器打开socket
 * nc -lk 9000
 * 2、启动程序
 * 3、在nc -lk 9000命令行
 * 在命令行窗口输入数据，比如：hello flink
 */
public class StreamWordCountJava {

    public static void main(String[] args) throws InterruptedException {
        // 第一步：创建SparkConf
        SparkConf conf = EnvUtils.buildSparkConfByEnv(StreamWordCountJava.class.getSimpleName());

        // 创建StreamingContext，指定数据处理间隔为5秒
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 通过socket获取实时产生的数据
        JavaReceiverInputDStream<String> linesRDD = ssc.socketTextStream("emon", 9000);

        // 对接收到的数据使用空格进行切割，转换成单个单词
        JavaDStream<String> wordsRDD =
                linesRDD.flatMap(
                        new FlatMapFunction<String, String>() {
                            @Override
                            public Iterator<String> call(String s) throws Exception {
                                return Arrays.stream(s.split(" ")).iterator();
                            }
                        });

        // 把每个单词转换成tuple2的形式
        JavaPairDStream<String, Integer> tupRDD =
                wordsRDD.mapToPair(
                        new PairFunction<String, String, Integer>() {
                            @Override
                            public Tuple2<String, Integer> call(String s) throws Exception {
                                return new Tuple2<>(s, 1);
                            }
                        });

        // 执行reduceByKey操作
        JavaPairDStream<String, Integer> wordCountRDD =
                tupRDD.reduceByKey(
                        new Function2<Integer, Integer, Integer>() {
                            @Override
                            public Integer call(Integer v1, Integer v2) throws Exception {
                                return (v1 + v2);
                            }
                        });

        // 将结果数据打印到控制台
        wordCountRDD.foreachRDD(
                new VoidFunction<JavaPairRDD<String, Integer>>() {
                    @Override
                    public void call(JavaPairRDD<String, Integer> pair) throws Exception {
                        pair.foreach(
                                new VoidFunction<Tuple2<String, Integer>>() {
                                    @Override
                                    public void call(Tuple2<String, Integer> tup) throws Exception {
                                        System.out.println(tup._1 + "-----" + tup._2);
                                    }
                                });
                    }
                });

        // 启动任务
        ssc.start();

        // 等待任务停止
        ssc.awaitTermination();
    }
}
