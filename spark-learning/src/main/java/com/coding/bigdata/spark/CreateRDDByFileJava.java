package com.coding.bigdata.spark;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class CreateRDDByFileJava {
    public static void main(String[] args) {
        // 第一步：创建SparkContext
        SparkConf conf = EnvUtils.buildSparkConfByEnv(CreateRDDByFileJava.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = "custom/data/spark/hello.txt";
        if (args.length == 1) {
            path = args[0];
        }

        // 读取文件数据，可以在 textFile 中指定生成的RDD的分区数量
        JavaRDD<String> linesRDD = sc.textFile(path, 2);

        // 获取每一行数据的长度，计算文件内数据的总长度
        Integer length =
                linesRDD.map((Function<String, Integer>) String::length)
                        .reduce((Function2<Integer, Integer, Integer>) Integer::sum);

        // 注意：这行println代码是在driver进程中执行的
        System.out.println(length);
    }
}
