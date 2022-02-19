package com.coding.bigdata.spark;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/*
 * 需求：Action实战
 * reduce：聚合计算==>将RDD中的所有元素进行聚合操作
 * collect：获取元素集合==>将RDD中所有元素获取到本地客户端（Driver）
 * take(n)：获取前n个元素==>获取RDD中前n个元素
 * count：获取元素总数==>获取RDD中元素总数
 * saveAsTextFile：保存文件==>将RDD中元素保存到文件中，对每个元素调用toString
 * countByKey：统计相同的key出现多少次==>对每个key对应的值进行count计数
 * foreach：迭代遍历元素==>遍历RDD中的每个元素
 */
public class ActionOpJava {

    public static void main(String[] args) {
        // 第一步：创建SparkContext
        SparkConf conf = EnvUtils.buildSparkConfByEnv(ActionOpJava.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        // reduce：聚合计算==>将RDD中的所有元素进行聚合操作
        //        reduceOp(sc);
        // collect：获取元素集合==>将RDD中所有元素获取到本地客户端（Driver）
        //        collectOp(sc);
        // take(n)：获取前n个元素==>获取RDD中前n个元素
        //        takeOp(sc);
        // count：获取元素总数==>获取RDD中元素总数
        //        countOp(sc);
        // saveAsTextFile：保存文件==>将RDD中元素保存到文件中，对每个元素调用toString
        //        saveAsTextFileOp(sc);
        // countByKey：统计相同的key出现多少次==>对每个key对应的值进行count计数
        //        countByKeyOp(sc);
        // foreach：迭代遍历元素==>遍历RDD中的每个元素
        foreachOp(sc);

        sc.stop();
    }

    private static void reduceOp(JavaSparkContext sc) {
        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        Integer num = dataRDD.reduce((Function2<Integer, Integer, Integer>) Integer::sum);
        System.out.println(num);
    }

    private static void collectOp(JavaSparkContext sc) {
        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        List<Integer> res = dataRDD.collect();
        for (Integer re : res) {
            System.out.println(re);
        }
    }

    private static void takeOp(JavaSparkContext sc) {
        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        List<Integer> res = dataRDD.take(2);
        for (Integer re : res) {
            System.out.println(re);
        }
    }

    private static void countOp(JavaSparkContext sc) {
        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        long count = dataRDD.count();
        System.out.println(count);
    }

    private static void saveAsTextFileOp(JavaSparkContext sc) {
        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        boolean isWinLocal = true;
        String path = "custom/data/spark/normal/output";
        if (!EnvUtils.isWin) {
            path = EnvUtils.toHDFSPath(path);
        }
        // 指定HDFS的路径信息即可，需要指定一个不存在的目录
        EnvUtils.checkOutputPath(path, isWinLocal);

        dataRDD.saveAsTextFile(path);
    }

    private static void countByKeyOp(JavaSparkContext sc) {
        Tuple2<String, Integer> t1 = new Tuple2<>("A", 1001);
        Tuple2<String, Integer> t2 = new Tuple2<>("B", 1002);
        Tuple2<String, Integer> t3 = new Tuple2<>("A", 1003);
        Tuple2<String, Integer> t4 = new Tuple2<>("C", 1004);
        JavaRDD<Tuple2<String, Integer>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3, t4));
        Map<String, Long> res =
                dataRDD.mapToPair(
                                (PairFunction<Tuple2<String, Integer>, String, Integer>)
                                        v1 -> new Tuple2<>(v1._1, v1._2))
                        .countByKey();
        for (Map.Entry<String, Long> entry : res.entrySet()) {
            System.out.println((entry.getKey() + "," + entry.getValue()));
        }
    }

    @SuppressWarnings("all")
    private static void foreachOp(JavaSparkContext sc) {
        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        dataRDD.foreach(item -> System.out.println(item));
    }
}
