package com.coding.bigdata.spark;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class MoreParallelismJava {

    @SuppressWarnings("all")
    public static void main(String[] args) {
        // 第一步：创建SparkContext
        SparkConf conf = EnvUtils.buildSparkConfByEnv(WordCountJava.class.getSimpleName());
        // 设置默认并行度为5
        conf.set("spark.default.parallelism", "5");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> dataRDD =
                sc.parallelize(
                        Arrays.asList(
                                "hello", "you", "hello", "me", "hehe", "hello", "you", "hello",
                                "me", "hehe"));
        dataRDD.mapToPair((PairFunction<String, String, Integer>) line -> new Tuple2<>(line, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum)
                .foreach(tup -> System.out.println(tup));

        sc.stop();
    }
}
