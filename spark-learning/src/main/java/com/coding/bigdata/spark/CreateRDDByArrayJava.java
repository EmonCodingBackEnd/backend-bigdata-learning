package com.coding.bigdata.spark;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

public class CreateRDDByArrayJava {
    public static void main(String[] args) {
        // 第一步：创建SparkContext
        SparkConf conf = EnvUtils.buildSparkConfByEnv(CreateRDDByArrayJava.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建集合
        List<Integer> arr = Arrays.asList(1, 2, 3, 4, 5, 6);
        // 基于集合创建RDD
        JavaRDD<Integer> rdd = sc.parallelize(arr);
        // 对集合中的数据求和
        Integer sum =
                rdd.reduce((Function2<Integer, Integer, Integer>) Integer::sum); // 这里创建的任务会在集群中执行

        // 注意：这行println代码是在driver进程中执行的
        System.out.println(sum);
    }
}
