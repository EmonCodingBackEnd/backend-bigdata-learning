package com.coding.bigdata.spark;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;

/*
 * 需求：foreachPartition的使用
 */
public class ForeachPartitionsOpJava {

    public static void main(String[] args) {
        // 第一步：创建SparkContext
        SparkConf conf = EnvUtils.buildSparkConfByEnv(AccumulatorOpJava.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);
        dataRDD.foreachPartition(
                (VoidFunction<Iterator<Integer>>)
                        it -> {
                            System.out.println("====================");
                            // 在此处获取数据库链接
                            while (it.hasNext()) {
                                // 在这里使用数据库链接
                                System.out.println(it.next());
                            }
                            // 在这里关闭数据库链接
                        });

        sc.stop();
    }
}
