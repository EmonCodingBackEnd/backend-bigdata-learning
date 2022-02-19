package com.coding.bigdata.spark;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;

/*
 * 需求：mapPartitions的使用
 */
public class MapPartitionsOpJava {

    public static void main(String[] args) {
        // 第一步：创建SparkContext
        SparkConf conf = EnvUtils.buildSparkConfByEnv(AccumulatorOpJava.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);

        // mapPartitions算子一次处理一个分区的数据
        /*Integer sum =
        dataRDD.map(
                        item -> {
                            System.out.println("====================");
                            return item * 2;
                        })
                .reduce(Integer::sum);*/

        Integer sum =
                dataRDD.mapPartitions(
                                it -> {
                                    System.out.println("====================");
                                    ArrayList<Integer> list = new ArrayList<>();
                                    // 这个foreach是调用的scala里面的函数
                                    while (it.hasNext()) {
                                        list.add(it.next() * 2);
                                    }
                                    return list.iterator();
                                })
                        .reduce(Integer::sum);

        System.out.println("sum = " + sum);

        sc.stop();
    }
}
