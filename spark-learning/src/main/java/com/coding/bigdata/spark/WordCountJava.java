package com.coding.bigdata.spark;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountJava {

    public static void main(String[] args) {
        // 第一步：创建SparkContext
        SparkConf conf = EnvUtils.buildSparkConfByEnv(WordCountJava.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 第二步：加载数据
        /*
         * linesRDD 内容：
         * hello you
         * hello me
         */
        String path = "custom/data/spark/hello.txt";
        if (args.length == 1) {
            path = args[0];
        }
        JavaRDD<String> linesRDD = sc.textFile(path);

        // 第三步：对数据进行切割，把一行数据切分
        /*
         * wordsRDD 内容：
         * hello
         * you
         * hello
         * me
         */
        JavaRDD<String> wordsRDD =
                linesRDD.flatMap(
                        (FlatMapFunction<String, String>)
                                line -> Arrays.asList(line.split(" ")).iterator());

        // 第四部：迭代words，将每个word转换为 (word,1) 这种形式
        /*
         * pairRDD 内容：
         * (hello,1)
         * (you,1)
         * (hello,1)
         * (me,1)
         */
        JavaPairRDD<String, Integer> pairRDD =
                wordsRDD.mapToPair(
                        (PairFunction<String, String, Integer>)
                                word -> new Tuple2<String, Integer>(word, 1));

        // 第五步：根据key(其实就是word）进行分组聚合
        /*
         * wordCountRDD 内容：
         * (me,1)
         * (you,1)
         * (hello,2)
         */
        JavaPairRDD<String, Integer> wordCountRDD =
                pairRDD.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);

        // 第六步：将结果打印到控制台
        wordCountRDD.foreach(
                (VoidFunction<Tuple2<String, Integer>>)
                        tuple2 -> System.out.println(tuple2._1 + "--" + tuple2._2));

        // 第七步：停止SparkContext
        sc.stop();
    }
}
