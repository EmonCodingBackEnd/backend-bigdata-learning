package com.coding.bigdata.spark;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.coding.bigdata.common.EnvUtils;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;

/*
 * 需求：TopN主播统计
 * 1：首先获取两份数据中的核心字段，使用fastjson包解析数据
 * 主播开播记录(video_info.log):主播ID：uid，直播间ID：vid，大区：area  一个主播一天可以开播多次，每次的直播间ID不一样
 * (vid,(uid,area))
 * 用户送礼记录(gift_record.log)：直播间ID：vid，金币数量：gold        一个直播间一天会接收到不同用户的礼物，也可能一个用户的多次礼物
 * (vid,gold)
 *
 * 这样的话可以把这两份数据关联到一块就能获取到大区、主播id、金币这些信息了，使用直播间vid进行关联
 *
 * 2：对用户送礼记录数据进行聚合，对相同vid的数据求和
 * 因为用户可能在一次直播中给主播送多次礼物
 * (vid,gold_sum)
 *
 * 3：把这两份数据join到一块，vid作为join的key
 * (vid,((uid,area),gold_sum))
 *
 * 4：使用map迭代join之后的数据，最后获取到uid、area、gold_sum字段
 * 由于一个主播一天可能会开播多次，后面需要基于uid和area再做一次聚合，所以把数据转换成这种格式
 *
 * uid和area是一一对应的，一个人只能属于大区
 * ((uid,area),gold_sum)
 *
 * 5：使用reduceByKey算子对数据进行聚合
 * ((uid,area),gold_sum_all)
 *
 * 6：接下来对需要使用groupByKey对数据进行分组，所以先使用map进行转换
 * 因为我们要分区统计TopN，所以要根据大区分组
 * map：(area,(uid,gold_sum_all))
 * groupByKey: area,<(uid,gold_sum_all),(uid,gold_sum_all),(uid,gold_sum_all)>
 *
 * 7：使用map迭代每个分组内的数据，按照金币数量倒序排序，取前N个，最终输出area,topN
 * 这个topN其实就是把前几名主播的id还有金币数量拼接成一个字符串
 * (area,topN)
 *
 * 8：使用foreach将结果打印到控制台，多个字段使用制表符分割
 */
public class TopNJava {

    public static void main(String[] args) {
        // 第一步：创建SparkContext
        SparkConf conf = EnvUtils.buildSparkConfByEnv(WordCountJava.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        String videoInfoPath = "custom/data/spark/topN/input/video_info.log";
        String giftRecordPath = "custom/data/spark/topN/input/gift_record.log";
        String top3Path = "custom/data/spark/topN/output";
        if (!EnvUtils.isWin) {
            videoInfoPath = EnvUtils.toHDFSPath(videoInfoPath);
            giftRecordPath = EnvUtils.toHDFSPath(giftRecordPath);
            top3Path = EnvUtils.toHDFSPath(top3Path);
        }

        // 1：首先获取两份数据中的核心字段，使用fastjson包解析数据
        /*
        数据示例：
        {'lm','hn','v01'}
        {'ls','hn','v02'}
        {'lm','hn','v03'}
        {'wh','ah','v04'}
        {'wc','ah','v05'}
         */
        JavaRDD<String> videoInfoRDD = sc.textFile(videoInfoPath);
        /*
        数据示例：
        {'v01',2}
        {'v01',3}
        {'v02',6}
        {'v02',10}
        {'v03',3}
        {'v04',10}
        {'v04',50}
        {'v05',20}
         */
        JavaRDD<String> giftRecordRDD = sc.textFile(giftRecordPath);

        /*
        数据示例：(vid, (uid, area))
        ('v01',('lm','hn'))
        ('v02',('ls','hn'))
        ('v03',('lm','hn'))
        ('v04',('wh','ah'))
        ('v05',('wc','ah'))
         */
        JavaPairRDD<String, Tuple2<String, String>> videoInfoFieldRDD =
                videoInfoRDD.mapToPair(
                        (PairFunction<String, String, Tuple2<String, String>>)
                                line -> {
                                    JSONObject jsonObj = JSON.parseObject(line);
                                    String uid = jsonObj.getString("uid");
                                    String vid = jsonObj.getString("vid");
                                    String area = jsonObj.getString("area");
                                    return new Tuple2<>(vid, new Tuple2<>(uid, area));
                                });

        /*
        数据示例：(vid, gold)
        ('v01',2)
        ('v01',3)
        ('v02',6)
        ('v02',10)
        ('v03',3)
        ('v04',10)
        ('v04',50)
        ('v05',20)
         */
        JavaPairRDD<String, Integer> giftRecordFieldRDD =
                giftRecordRDD.mapToPair(
                        (PairFunction<String, String, Integer>)
                                line -> {
                                    JSONObject jsonObj = JSON.parseObject(line);
                                    String vid = jsonObj.getString("vid");
                                    Integer gold = Integer.parseInt(jsonObj.getString("gold"));
                                    return new Tuple2<>(vid, gold);
                                });

        // 2：对用户送礼记录数据进行聚合，对相同vid的数据求和
        /*
        数据示例：(vid, gold_sum)
        ('v01',5)
        ('v02',16)
        ('v03',3)
        ('v04',60)
        ('v05',20)
         */
        JavaPairRDD<String, Integer> giftRecordFieldAggRDD =
                giftRecordFieldRDD.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);

        // 3：把这两份数据join到一块，vid作为join的key
        /*
        数据实力：(vid, ((uid,area),gold_sum))
        ('v01', (('lm', 'hn'), 5))
        ('v02', (('ls', 'hn'), 16))
        ('v03', (('lm', 'hn'), 3))
        ('v04', (('wh', 'ah'), 60))
        ('v05', (('wc', 'ah'), 20))
         */
        // JavaPairRDD<String, Tuple2<Tuple2<String, String>, Integer>> joinRDD =
        // videoInfoFieldRDD.join(giftRecordFieldAggRDD);

        // 4：使用map迭代join之后的数据，最后获取到uid、area、gold_sum字段
        // ((uid,area),gold_sum)
        /*
        数据示例：((uid,area),gold_sum)
        (('lm', 'hn'), 5)
        (('ls', 'hn'), 16)
        (('lm', 'hn'), 3)
        (('wh', 'ah'), 60)
        (('wc', 'ah'), 20)
         */
        // 由于join操作的结果在Java中提取出来会导致编译不通过，所以直接联合3和4两个步骤合二为一。
        JavaPairRDD<Tuple2<String, String>, Integer> joinMapRDD =
                videoInfoFieldRDD
                        .join(giftRecordFieldAggRDD)
                        .mapToPair(
                                (PairFunction<
                                                Tuple2<
                                                        String,
                                                        Tuple2<Tuple2<String, String>, Integer>>,
                                                Tuple2<String, String>,
                                                Integer>)
                                        tup -> {
                                            //  joinRDD: (vid, ((uid,area),gold_sum)) =>
                                            // ((uid,area),gold_sum)
                                            String uid = tup._2._1._1;
                                            String area = tup._2._1._2;
                                            Integer gold_sum = tup._2._2;
                                            return new Tuple2<>(new Tuple2<>(uid, area), gold_sum);
                                        });

        // 5：使用reduceByKey算子对数据进行聚合
        // ((uid,area),gold_sum_all)
        /*
        数据示例：((uid,area),gold_sum_all)
        ('lm', 'hn'), 8)
        ('ls', 'hn'), 16)
        ('wh', 'ah'), 60)
        ('wc', 'ah'), 20)
         */
        JavaPairRDD<Tuple2<String, String>, Integer> reduceRDD =
                joinMapRDD.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);

        // 6：接下来对需要使用groupByKey对数据进行分组，所以先使用map进行转换
        /*
        数据示例：
        map转换后==>(area, (uid,gold_sum_all)
        ('hn', ('lm', 8))
        ('hn', ('ls', 16))
        ('ah', ('wh', 60))
        ('ah', ('wc', 20))
        groupByKey转换后==>(area, <(uid,gold_sum_all),(uid,gold_sum_all),(uid,gold_sum_all)>)
        ('hn', <('lm', 8), ('ls', 16)>)
        ('ah', <('wh', 60), ('wc', 20)>)
         */
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupRDD =
                reduceRDD
                        .mapToPair(
                                (PairFunction<
                                                Tuple2<Tuple2<String, String>, Integer>,
                                                String,
                                                Tuple2<String, Integer>>)
                                        tup -> {
                                            return new Tuple2<>(
                                                    tup._1._2, new Tuple2<>(tup._1._1, tup._2));
                                        })
                        .groupByKey();

        // 7：使用map迭代每个分组内的数据，按照金币数量倒序排序，取前N个，最终输出area,topN
        // (area,topN)
        JavaRDD<Tuple2<String, String>> top3RDD =
                groupRDD.map(
                        (Function<
                                        Tuple2<String, Iterable<Tuple2<String, Integer>>>,
                                        Tuple2<String, String>>)
                                tup -> {
                                    String area = tup._1;
                                    ArrayList<Tuple2<String, Integer>> tupleList =
                                            Lists.newArrayList(tup._2);
                                    tupleList.sort(
                                            new Comparator<Tuple2<String, Integer>>() {
                                                @Override
                                                public int compare(
                                                        Tuple2<String, Integer> o1,
                                                        Tuple2<String, Integer> o2) {
                                                    // 倒序
                                                    return o2._2 - o1._2;
                                                }
                                            });

                                    /*
                                    数据示例：(area, top3)
                                    针对'hn'区域： <('lm', 8), ('ls', 16)> sortBy并reverse后==> [('ls', 16), ('lm', 8)] map后==> "'ls':16,'lm':8"
                                    针对'ah'区域： <('wh', 60), ('wc', 20)> sortBy并reverse后==> [('wh', 60), ('wc', 20)] map后==> "'wh':60,'wc':20"
                                    ('hn',"'ls':16,'lm':8")
                                    ('ah',"'wh':60,'wc':20")
                                     */
                                    StringBuilder sb = new StringBuilder();
                                    for (int i = 0; i < tupleList.size(); i++) {
                                        if (i < 3) { // top3
                                            Tuple2<String, Integer> t = tupleList.get(i);
                                            if (i != 0) {
                                                sb.append(",");
                                            }
                                            sb.append(t._1).append(":").append(t._2);
                                        }
                                    }
                                    return new Tuple2<>(area, sb.toString());
                                });

        // 8：使用foreach将结果打印到控制台，多个字段使用制表符分割
        /*
        数据示例：
        hn  "'ls':16,'lm':8"
        ah  "'wh':60,'wc':20"
         */
        if (EnvUtils.isWin) {
            top3RDD.foreach(tup -> System.out.println(tup._1 + ":" + tup._2));
        } else {
            // 指定HDFS的路径信息即可，需要指定一个不存在的目录
            EnvUtils.checkOutputPath(top3Path, false);
            top3RDD.saveAsTextFile(top3Path);
        }

        sc.stop();
    }
}
