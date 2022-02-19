package com.coding.bigdata.spark

import com.coding.bigdata.common.EnvScalaUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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
object TransformationOpScala {


  def main(args: Array[String]): Unit = {
    // 第一步：创建SparkContext
    val conf = EnvScalaUtils.buildSparkConfByEnv(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    // map：对集合中每个元素乘以2
    //    mapOp(sc)
    // filter：过滤出集合中的偶数
    //    filterOp(sc)
    // flatMap：将行拆分为单词
    //    flatMapOp(sc)
    // groupByKey：对每个大区的主播进行分组
    //    groupByKeyOp(sc)
    //    groupByKeyOp2(sc)
    //    groupByOp2(sc)
    // reduceByKey：统计每个大区的主播数量
    //    reduceByKeyOp(sc)
    // sortByKey：对主播的音浪收入排序
    //    sortByKey(sc)
    //    sortByKey2(sc)
    // join：打印每个主播的大区信息和音浪收入
    joinOp(sc)
    // distinct：统计当天开播的大区信息
    //    distinctOp(sc)

    sc.stop()
  }

  def mapOp(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5))
    dataRDD.map(_ * 2).foreach(println(_))
  }

  def filterOp(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5))
    dataRDD.filter(_ % 2 == 0).foreach(println(_))
  }

  def flatMapOp(sc: SparkContext): Unit = {
    val dataRDD: RDD[String] = sc.parallelize(Array("good good study", "day day up"))
    dataRDD.flatMap(_.split(" ")).foreach(println(_))
  }

  def groupByKeyOp(sc: SparkContext): Unit = {
    val dataRDD: RDD[(Int, String)] = sc.parallelize(Array((150001, "US"), (150002, "CN"), (150003, "CN"), (150004, "IN")))

    // 需要使用map对tuple中的数据进行位置互换，因为我们需要把大区作为key进行分组操作
    // 此时的key就是tuple中的第一列，其实在这里就可以把这个tuple认为是一个key-value
    // 注意：在使用类似于groupByKey这种基于key的算子时，需要提前把RDD中的数据组装成tuple2这种形式
    val groupRDD: RDD[(String, Iterable[Int])] = dataRDD.map(tup => (tup._2, tup._1)).groupByKey()
    groupRDD.foreach(tup => println(tup._1 + ":" + tup._2.mkString(" ")))
  }

  def groupByKeyOp2(sc: SparkContext): Unit = {
    val dataRDD: RDD[(Int, String, String)] = sc.parallelize(Array((150001, "US", "male"), (150002, "CN", "female"), (150003, "CN", "male"), (150004, "IN", "female")))
    val groupRDD: RDD[(String, Iterable[(Int, String)])] = dataRDD.map(tup => (tup._2, (tup._1, tup._3))).groupByKey()
    groupRDD.foreach(tup => println(tup._1 + ":" + tup._2.mkString(" ")))
  }

  def groupByOp2(sc: SparkContext): Unit = {
    val dataRDD: RDD[(Int, String, String)] = sc.parallelize(Array((150001, "US", "male"), (150002, "CN", "female"), (150003, "CN", "male"), (150004, "IN", "female")))
    val groupRDD: RDD[(String, Iterable[(Int, String, String)])] = dataRDD.groupBy(_._2)
    groupRDD.foreach(tup => println(tup._1 + ":" + tup._2.mkString(" ")))
  }

  def reduceByKeyOp(sc: SparkContext): Unit = {
    val dataRDD: RDD[(Int, String)] = sc.parallelize(Array((150001, "US"), (150002, "CN"), (150003, "CN"), (150004, "IN")))
    // 由于这个需求只需要使用到大区信息，所以在map操作的时候只保留大区信息即可
    dataRDD.map(tup => (tup._2, 1)).reduceByKey((x, y) => x + y).foreach(println(_))
  }

  // 会全局排序
  def sortByKey(sc: SparkContext): Unit = {
    val dataRDD: RDD[(Int, Int)] = sc.parallelize(Array((150001, 400), (150002, 200), (150003, 300), (150004, 100)))
    // 由于需要对音浪收入进行排序，所以需要把音浪收入作为key，在这里需要进行位置的互换
    dataRDD.map(tup => (tup._2, tup._1))
      // 默认是正序，第一个参数为true，想要倒序需要把这个参数设置为false
      .sortByKey(ascending = false)
      .foreach(println(_))
  }

  // 会全局排序
  def sortByKey2(sc: SparkContext): Unit = {
    val dataRDD: RDD[(Int, Int)] = sc.parallelize(Array((150001, 400), (150002, 200), (150003, 300), (150004, 100)))
    // 由于需要对音浪收入进行排序，所以需要把音浪收入作为key，在这里需要进行位置的互换
    dataRDD.sortBy(_._2, ascending = false).foreach(println(_))
  }

  def joinOp(sc: SparkContext): Unit = {
    val dataRDD1: RDD[(Int, String)] = sc.parallelize(Array((150001, "US"), (150002, "CN"), (150003, "CN"), (150004, "IN")))
    val dataRDD2: RDD[(Int, Int)] = sc.parallelize(Array((150001, 400), (150002, 200), (150003, 300), (150004, 100)))
    // [(150001,(US,400)), (150002,(CN,200)),(150003,(CN,300)),(150004,(IN,100))
    val joinRDD: RDD[(Int, (String, Int))] = dataRDD1.join(dataRDD2)
    joinRDD.foreach(tup => {
      // 用户id
      val uid = tup._1
      val area_gold = tup._2
      // 大区
      val area = area_gold._1
      // 音浪收入
      val gold = area_gold._2
      println(uid + "\t" + area + "\t" + gold)
    })
  }

  def distinctOp(sc: SparkContext): Unit = {
    val dataRDD: RDD[(Int, String)] = sc.parallelize(Array((150001, "US"), (150002, "CN"), (150003, "CN"), (150004, "IN")))
    dataRDD.map(_._2).distinct().foreach(println(_))
  }
}
