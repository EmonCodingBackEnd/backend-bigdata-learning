package com.coding.bigdata.spark;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;

/*
 * 需求：使用广播变量
 */
public class BroadcastOpJava {

    @SuppressWarnings("all")
    public static void main(String[] args) {
        // 第一步：创建SparkContext
        SparkConf conf = EnvUtils.buildSparkConfByEnv(BroadcastOpJava.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        int variable = 2;

        // 直接使用普通外部变量，会拷贝到task
        // dataRDD.map((Function<Integer, Integer>) v1 -> v1 * variable);

        // 1、定义广播变量
        Broadcast<Integer> variableBroadcast = sc.broadcast(variable);

        // 2、使用广播变量，调用其他value方法
        dataRDD.map((Function<Integer, Integer>) v1 -> v1 * variableBroadcast.value())
                .foreach((VoidFunction<Integer>) v1 -> System.out.println(v1));

        sc.stop();
    }
}
