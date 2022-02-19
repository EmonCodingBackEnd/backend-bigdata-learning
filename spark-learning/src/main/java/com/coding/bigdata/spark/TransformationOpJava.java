package com.coding.bigdata.spark;

import com.coding.bigdata.common.EnvUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;

/*
 * 需求：Transformation实战
 * map：对集合中每个元素乘以2
 * filter：过滤出集合中的偶数
 * flatMap：将行拆分为单词
 * groupByKey：对每个大区的主播进行分组
 * reduceByKey：统计每个大区的主播数量
 * sortByKey：对主播的音浪收入排序
 * join：打印每个主播的大区信息和音浪收入
 * distinct：统计当天开播的大区信息
 */
public class TransformationOpJava {

    public static void main(String[] args) {
        // 第一步：创建SparkContext
        SparkConf conf = EnvUtils.buildSparkConfByEnv(TransformationOpJava.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        // map：对集合中每个元素乘以2
        mapOp(sc);
        // filter：过滤出集合中的偶数
        //        filterOp(sc);
        // flatMap：将行拆分为单词
        //        flatMapOp(sc);
        // groupByKey：对每个大区的主播进行分组
        //        groupByKeyOp(sc);
        //        groupByKeyOp2(sc);
        //    groupByOp2(sc)
        // reduceByKey：统计每个大区的主播数量
        //        reduceByKeyOp(sc);
        // sortByKey：对主播的音浪收入排序
        //        sortByKey(sc);
        //        sortByKey2(sc);
        // join：打印每个主播的大区信息和音浪收入
        //        joinOp(sc);
        // distinct：统计当天开播的大区信息
        //        distinctOp(sc);

        sc.stop();
    }

    @SuppressWarnings("all")
    private static void mapOp(JavaSparkContext sc) {
        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        dataRDD
                // .map((Function<Integer, Integer>) v1 -> v1 * 2)
                // 可以使用简化的map替换
                .map(v1 -> v1 * 2)
                // 绝对不可用 System.out::println 替换，否则会报错：
                // Exception in thread "main" org.apache.spark.SparkException: Task not serializable
                .foreach((VoidFunction<Integer>) v1 -> System.out.println(v1));
    }

    @SuppressWarnings("all")
    private static void filterOp(JavaSparkContext sc) {
        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        dataRDD.filter((Function<Integer, Boolean>) v1 -> v1 % 2 == 0)
                .foreach((VoidFunction<Integer>) v1 -> System.out.println(v1));
    }

    @SuppressWarnings("all")
    private static void flatMapOp(JavaSparkContext sc) {
        JavaRDD<String> dataRDD = sc.parallelize(Arrays.asList("good good study", "day day up"));
        dataRDD.flatMap(
                        (FlatMapFunction<String, String>)
                                v1 -> Arrays.stream(v1.split(" ")).iterator())
                .foreach((VoidFunction<String>) v1 -> System.out.println(v1));
    }

    private static void groupByKeyOp(JavaSparkContext sc) {
        Tuple2<Integer, String> t1 = new Tuple2<>(150001, "US");
        Tuple2<Integer, String> t2 = new Tuple2<>(150002, "CN");
        Tuple2<Integer, String> t3 = new Tuple2<>(150003, "CN");
        Tuple2<Integer, String> t4 = new Tuple2<>(150004, "IN");
        JavaRDD<Tuple2<Integer, String>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3, t4));
        dataRDD.mapToPair(
                        (PairFunction<Tuple2<Integer, String>, String, Integer>)
                                tup -> new Tuple2<>(tup._2, tup._1))
                .groupByKey()
                .foreach(
                        (VoidFunction<Tuple2<String, Iterable<Integer>>>)
                                tup -> {
                                    String info = tup._1 + ":" + StringUtils.join(tup._2, " ");
                                    System.out.println(info);
                                });
    }

    private static void groupByKeyOp2(JavaSparkContext sc) {
        Tuple3<Integer, String, String> t1 = new Tuple3<>(150001, "US", "male");
        Tuple3<Integer, String, String> t2 = new Tuple3<>(150002, "CN", "female");
        Tuple3<Integer, String, String> t3 = new Tuple3<>(150003, "CN", "male");
        Tuple3<Integer, String, String> t4 = new Tuple3<>(150004, "IN", "female");
        JavaRDD<Tuple3<Integer, String, String>> dataRDD =
                sc.parallelize(Arrays.asList(t1, t2, t3, t4));
        dataRDD.mapToPair(
                        (PairFunction<
                                        Tuple3<Integer, String, String>,
                                        String,
                                        Tuple2<Integer, String>>)
                                tup -> new Tuple2<>(tup._2(), new Tuple2<>(tup._1(), tup._3())))
                .groupByKey()
                .foreach(
                        (VoidFunction<Tuple2<String, Iterable<Tuple2<Integer, String>>>>)
                                tup -> {
                                    String info = tup._1 + ":" + StringUtils.join(tup._2, " ");
                                    System.out.println(info);
                                });
    }

    @SuppressWarnings("all")
    private static void reduceByKeyOp(JavaSparkContext sc) {
        Tuple2<Integer, String> t1 = new Tuple2<>(150001, "US");
        Tuple2<Integer, String> t2 = new Tuple2<>(150002, "CN");
        Tuple2<Integer, String> t3 = new Tuple2<>(150003, "CN");
        Tuple2<Integer, String> t4 = new Tuple2<>(150004, "IN");
        JavaRDD<Tuple2<Integer, String>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3, t4));
        dataRDD.mapToPair(
                        (PairFunction<Tuple2<Integer, String>, String, Integer>)
                                tup -> new Tuple2<>(tup._2, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum)
                .foreach((VoidFunction<Tuple2<String, Integer>>) v1 -> System.out.println(v1));
    }

    @SuppressWarnings("all")
    private static void sortByKey(JavaSparkContext sc) {
        Tuple2<Integer, Integer> t1 = new Tuple2<>(150001, 400);
        Tuple2<Integer, Integer> t2 = new Tuple2<>(150002, 300);
        Tuple2<Integer, Integer> t3 = new Tuple2<>(150003, 200);
        Tuple2<Integer, Integer> t4 = new Tuple2<>(150004, 100);
        JavaRDD<Tuple2<Integer, Integer>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3, t4));
        dataRDD.mapToPair(
                        (PairFunction<Tuple2<Integer, Integer>, Integer, Integer>)
                                tup -> new Tuple2<>(tup._2, tup._1))
                .sortByKey(false)
                .foreach((VoidFunction<Tuple2<Integer, Integer>>) v1 -> System.out.println(v1));
    }

    @SuppressWarnings("all")
    private static void sortByKey2(JavaSparkContext sc) {
        Tuple2<Integer, Integer> t1 = new Tuple2<>(150001, 400);
        Tuple2<Integer, Integer> t2 = new Tuple2<>(150002, 300);
        Tuple2<Integer, Integer> t3 = new Tuple2<>(150003, 200);
        Tuple2<Integer, Integer> t4 = new Tuple2<>(150004, 100);
        JavaRDD<Tuple2<Integer, Integer>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3, t4));
        dataRDD.sortBy(
                        new Function<Tuple2<Integer, Integer>, Integer>() {
                            @Override
                            public Integer call(Tuple2<Integer, Integer> v1) throws Exception {
                                return v1._2;
                            }
                        },
                        false,
                        1)
                .foreach((VoidFunction<Tuple2<Integer, Integer>>) v1 -> System.out.println(v1));
    }

    @SuppressWarnings("all")
    private static void joinOp(JavaSparkContext sc) {
        Tuple2<Integer, String> t1 = new Tuple2<>(150001, "US");
        Tuple2<Integer, String> t2 = new Tuple2<>(150002, "CN");
        Tuple2<Integer, String> t3 = new Tuple2<>(150003, "CN");
        Tuple2<Integer, String> t4 = new Tuple2<>(150004, "IN");
        JavaRDD<Tuple2<Integer, String>> dataRDD1 = sc.parallelize(Arrays.asList(t1, t2, t3, t4));

        Tuple2<Integer, Integer> t21 = new Tuple2<>(150001, 400);
        Tuple2<Integer, Integer> t22 = new Tuple2<>(150002, 300);
        Tuple2<Integer, Integer> t23 = new Tuple2<>(150003, 200);
        Tuple2<Integer, Integer> t24 = new Tuple2<>(150004, 100);
        JavaRDD<Tuple2<Integer, Integer>> dataRDD2 =
                sc.parallelize(Arrays.asList(t21, t22, t23, t24));

        JavaPairRDD<Integer, String> pairRDD1 =
                dataRDD1.mapToPair(
                        (PairFunction<Tuple2<Integer, String>, Integer, String>)
                                tup -> new Tuple2<>(tup._1, tup._2));
        JavaPairRDD<Integer, Integer> pairRDD2 =
                dataRDD2.mapToPair(
                        (PairFunction<Tuple2<Integer, Integer>, Integer, Integer>)
                                tup -> new Tuple2<>(tup._1, tup._2));
        pairRDD1.join(pairRDD2)
                .foreach(
                        (VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>)
                                v1 -> System.out.println(v1));
    }

    @SuppressWarnings("all")
    private static void distinctOp(JavaSparkContext sc) {
        Tuple2<Integer, String> t1 = new Tuple2<>(150001, "US");
        Tuple2<Integer, String> t2 = new Tuple2<>(150002, "CN");
        Tuple2<Integer, String> t3 = new Tuple2<>(150003, "CN");
        Tuple2<Integer, String> t4 = new Tuple2<>(150004, "IN");
        JavaRDD<Tuple2<Integer, String>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3, t4));
        dataRDD.map((Function<Tuple2<Integer, String>, String>) tup -> tup._2)
                .distinct()
                .foreach((VoidFunction<String>) v1 -> System.out.println(v1));
    }
}
