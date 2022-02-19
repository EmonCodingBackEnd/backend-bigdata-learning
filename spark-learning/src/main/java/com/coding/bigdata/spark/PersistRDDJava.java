package com.coding.bigdata.spark;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/*
 * 需求：RDD持久化
 */
public class PersistRDDJava {

    public static void main(String[] args) {
        // 第一步：创建SparkContext
        SparkConf conf = EnvUtils.buildSparkConfByEnv(PersistRDDJava.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = "custom/data/mr/skew/input/hello_10000000.dat";
        JavaRDD<String> dataRDD = sc.textFile(path).cache();

        long startTime = System.currentTimeMillis();
        long count = dataRDD.count();
        System.out.println(count);
        long endTime = System.currentTimeMillis();
        System.out.println("第一次耗时：" + (endTime - startTime));

        startTime = System.currentTimeMillis();
        count = dataRDD.count();
        System.out.println(count);
        endTime = System.currentTimeMillis();
        System.out.println("第二次耗时：" + (endTime - startTime));

        sc.stop();
    }
}
