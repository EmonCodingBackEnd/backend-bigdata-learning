package com.coding.bigdata.spark;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;

/*
 * 需求：repartition的使用
 */
public class RepartitionOpJava {

    public static void main(String[] args) {
        // 第一步：创建SparkContext
        SparkConf conf = EnvUtils.buildSparkConfByEnv(RepartitionOpJava.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);
        dataRDD.repartition(3)
                .foreachPartition(
                        (VoidFunction<Iterator<Integer>>)
                                it -> {
                                    System.out.println("====================");
                                    while (it.hasNext()) {
                                        System.out.println(it.next());
                                    }
                                });

        // 通过repartition可以控制输出数据产生的文件个数
        {
            boolean isWinLocal = true;
            String path = "custom/data/spark/normal/output";
            if (!EnvUtils.isWin) {
                path = EnvUtils.toHDFSPath(path);
            }
            // 指定HDFS的路径信息即可，需要指定一个不存在的目录
            EnvUtils.checkOutputPath(path, isWinLocal);

            dataRDD.repartition(1).saveAsTextFile(path);
        }

        sc.stop();
    }
}
