package com.coding.bigdata.spark;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

/*
 * 需求：使用累加变量
 */
public class AccumulatorOpJava {

    public static void main(String[] args) {
        // 第一步：创建SparkContext
        SparkConf conf = EnvUtils.buildSparkConfByEnv(AccumulatorOpJava.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        // 1、定义累加变量
        LongAccumulator sumAccumulator = sc.sc().longAccumulator();

        // 2、使用累加变量
        dataRDD.foreach(sumAccumulator::add);

        // 注意：只能在Driver进程中获取累加变量的结果
        System.out.println(sumAccumulator.value());

        sc.stop();
    }
}
