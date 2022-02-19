package com.coding.bigdata.spark

import com.alibaba.fastjson.JSON
import com.coding.bigdata.common.{EnvScalaUtils, EnvUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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
 *
 * 任务提交脚本：
 * 创建脚本： [emon@emon ~]$ vim /home/emon/bigdata/spark/shell/topNScala.sh
 * 脚本内容：如下：
spark-submit \
--class com.coding.bigdata.spark.TopNScala \
--packages com.alibaba:fastjson:1.2.79 \
--master yarn \
--deploy-mode client \
--executor-memory 1G \
--num-executors 1 \
~/bigdata/spark/lib/spark-learning-1.0-SNAPSHOT-jar-with-dependencies.jar
 * 修改脚本可执行权限：[emon@emon ~]$ chmod u+x /home/emon/bigdata/spark/shell/topNScala.sh
 * 执行脚本：[emon@emon ~]$ sh -x /home/emon/bigdata/spark/shell/topNScala.sh
 */
object TopNScala {

  def main(args: Array[String]): Unit = {
    // 第一步：创建SparkContext
    val conf = EnvScalaUtils.buildSparkConfByEnv(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    var videoInfoPath = "custom/data/spark/topN/input/video_info.log"
    var giftRecordPath = "custom/data/spark/topN/input/gift_record.log"
    var top3Path = "custom/data/spark/topN/output"
    if (!EnvUtils.isWin) {
      videoInfoPath = EnvUtils.toHDFSPath(videoInfoPath)
      giftRecordPath = EnvUtils.toHDFSPath(giftRecordPath)
      top3Path = EnvUtils.toHDFSPath(top3Path)
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
    val videoInfoRDD: RDD[String] = sc.textFile(videoInfoPath)

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
    val giftRecordRDD: RDD[String] = sc.textFile(giftRecordPath)

    /*
    数据示例：(vid, (uid, area))
    ('v01',('lm','hn'))
    ('v02',('ls','hn'))
    ('v03',('lm','hn'))
    ('v04',('wh','ah'))
    ('v05',('wc','ah'))
     */
    val videoInfoFieldRDD: RDD[(String, (String, String))] = videoInfoRDD.map(line => {
      val jsonObj = JSON.parseObject(line)
      val uid = jsonObj.getString("uid")
      val vid = jsonObj.getString("vid")
      val area = jsonObj.getString("area")
      (vid, (uid, area))
    })

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
    val giftRecordFieldRDD: RDD[(String, Int)] = giftRecordRDD.map(line => {
      val jsonObj = JSON.parseObject(line)
      val vid = jsonObj.getString("vid")
      val gold = Integer.parseInt(jsonObj.getString("gold"))
      (vid, gold)
    })

    // 2：对用户送礼记录数据进行聚合，对相同vid的数据求和
    /*
    数据示例：(vid, gold_sum)
    ('v01',5)
    ('v02',16)
    ('v03',3)
    ('v04',60)
    ('v05',20)
     */
    val giftRecordFieldAggRDD: RDD[(String, Int)] = giftRecordFieldRDD.reduceByKey(_ + _)

    // 3：把这两份数据join到一块，vid作为join的key
    /*
    数据实力：(vid, ((uid,area),gold_sum))
    ('v01', (('lm', 'hn'), 5))
    ('v02', (('ls', 'hn'), 16))
    ('v03', (('lm', 'hn'), 3))
    ('v04', (('wh', 'ah'), 60))
    ('v05', (('wc', 'ah'), 20))
     */
    val joinRDD: RDD[(String, ((String, String), Int))] = videoInfoFieldRDD.join(giftRecordFieldAggRDD)

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
    val joinMapRDD: RDD[((String, String), Int)] = joinRDD.map(tup => {
      //  joinRDD: (vid, ((uid,area),gold_sum)) => ((uid,area),gold_sum)
      val uid = tup._2._1._1
      val area = tup._2._1._2
      val gold_sum = tup._2._2
      ((uid, area), gold_sum)
    })

    // 5：使用reduceByKey算子对数据进行聚合
    // ((uid,area),gold_sum_all)
    /*
    数据示例：((uid,area),gold_sum_all)
    ('lm', 'hn'), 8)
    ('ls', 'hn'), 16)
    ('wh', 'ah'), 60)
    ('wc', 'ah'), 20)
     */
    val reduceRDD: RDD[((String, String), Int)] = joinMapRDD.reduceByKey(_ + _)

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
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = reduceRDD.map(tup => (tup._1._2, (tup._1._1, tup._2))).groupByKey()

    // 7：使用map迭代每个分组内的数据，按照金币数量倒序排序，取前N个，最终输出area,topN
    // (area,topN)
    val top3RDD: RDD[(String, String)] = groupRDD.map(tup => {
      val area = tup._1
      /*
      数据示例：(area, top3)
      针对'hn'区域： <('lm', 8), ('ls', 16)> sortBy并reverse后==> [('ls', 16), ('lm', 8)] map后==> "'ls':16,'lm':8"
      针对'ah'区域： <('wh', 60), ('wc', 20)> sortBy并reverse后==> [('wh', 60), ('wc', 20)] map后==> "'wh':60,'wc':20"
      ('hn',"'ls':16,'lm':8")
      ('ah',"'wh':60,'wc':20")
       */
      val top3 = tup._2.toList.sortBy(_._2).reverse.take(3).map(tup => tup._1 + ":" + tup._2).mkString(",")
      (area, top3)
    })

    // 8：使用foreach将结果打印到控制台，多个字段使用制表符分割
    /*
    数据示例：
    hn  "'ls':16,'lm':8"
    ah  "'wh':60,'wc':20"
     */
    if (EnvUtils.isWin) {
      top3RDD.foreach(tup => println(tup._1 + "\t" + tup._2))
    } else {
      // 指定HDFS的路径信息即可，需要指定一个不存在的目录
      EnvUtils.checkOutputPath(top3Path, false);
      top3RDD.saveAsTextFile(top3Path)
    }

    sc.stop()
  }
}
