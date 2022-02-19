package com.coding.bigdata.spark

import com.coding.bigdata.common.{EnvScalaUtils, EnvUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


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
object ActionOpScala {


  def main(args: Array[String]): Unit = {
    // 第一步：创建SparkContext
    val conf = EnvScalaUtils.buildSparkConfByEnv(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    // reduce：聚合计算==>将RDD中的所有元素进行聚合操作
    //    reduceOp(sc)
    // collect：获取元素集合==>将RDD中所有元素获取到本地客户端（Driver）
    //    collectOp(sc)
    // take(n)：获取前n个元素==>获取RDD中前n个元素
    //    takeOp(sc)
    // count：获取元素总数==>获取RDD中元素总数
    //    countOp(sc)
    // saveAsTextFile：保存文件==>将RDD中元素保存到文件中，对每个元素调用toString
    //    saveAsTextFileOp(sc)
    // countByKey：统计相同的key出现多少次==>对每个key对应的值进行count计数
    //    countByKeyOp(sc)
    // foreach：迭代遍历元素==>遍历RDD中的每个元素
    foreachOp(sc)

    sc.stop()
  }

  def reduceOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array(1, 2, 3, 4, 5))
    val num = dataRDD.reduce(_ + _)
    println(num)
  }

  def collectOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array(1, 2, 3, 4, 5))
    // collect返回的是一个Array数组
    // 注意：如果RDD中数据量过大，不建议使用collect，因为最终的数据会返回给Driver进程所在节点
    // 如果想要获取几条数据，查看一下数据格式，可以使用take(n)
    val res: Array[Int] = dataRDD.collect()
    for (item <- res) {
      println(item)
    }
  }

  def takeOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array(1, 2, 3, 4, 5))
    val res: Array[Int] = dataRDD.take(2)
    for (elem <- res) {
      println(elem)
    }
  }

  def countOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array(1, 2, 3, 4, 5))
    val res = dataRDD.count()
    println(res)
  }

  def saveAsTextFileOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array(1, 2, 3, 4, 5))

    val isWinLocal = true
    var path = "custom/data/spark/normal/output"
    if (!EnvUtils.isWin) {
      path = EnvUtils.toHDFSPath(path)
    }
    // 指定HDFS的路径信息即可，需要指定一个不存在的目录
    EnvUtils.checkOutputPath(path, isWinLocal)

    dataRDD.saveAsTextFile(path)
  }

  def countByKeyOp(sc: SparkContext): Unit = {
    val dataRDD: RDD[(String, Int)] = sc.parallelize(Array(("A", 1001), ("B", 1002), ("A", 1003), ("C", 1004)))
    val res: collection.Map[String, Long] = dataRDD.countByKey()
    for ((k, v) <- res) {
      println(k + "," + v)
    }
  }

  def foreachOp(sc: SparkContext): Unit = {
    val dataRDD = sc.parallelize(Array(1, 2, 3, 4, 5))
    // 注意：foreach不仅限于执行println操作，这是知识在测试的时候使用的
    // 实际工作中如果需要把计算的结果保存到第三方的存储介质中，就需要使用foreach
    // 在里面实现具体向外部输出数据的代码
    dataRDD.foreach(item => {
      println(item)
    })
  }


}
