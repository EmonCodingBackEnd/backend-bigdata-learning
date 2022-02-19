package com.coding.bigdata.spark

import com.coding.bigdata.common.EnvScalaUtils
import org.apache.spark.SparkContext

/*
 * 需求：设置并行度
 * 1、可以在textFile或者是parallelize方法的第二个参数中设置并行度
 * 2、可以通过 spark.default.parallelism 参数统一设置并行度
 * Spark官方推荐，给集群中的每个cpu core设置2-3个task；
 * 如果--num-executors 3且--executor-cores 2，那么应该设置12-18个并行度，也就是：--conf "spark.default.parallelism=12"
 * 注意：yarn集群节点中，所有节点的逻辑CPU个数（物理CPU个数*每个CPU的Core数）>= num-executors
 *
 * spark-submit参数解释：
 * --executor-memory 1G 每个节点分配的内存数==>每个executor=该值/每个节点上分配的executors数量
 * --num-executors 3    给这个任务分配了5个executor
 * --executor-cores 2   每个executor，分配了2个cpu core
 *
 * 任务提交脚本：
 * 创建脚本： [emon@emon ~]$ vim /home/emon/bigdata/spark/shell/moreParallelismScala.sh
 * 脚本内容：如下：
spark-submit \
--class com.coding.bigdata.spark.MoreParallelismScala \
--master yarn \
--deploy-mode client \
--executor-memory 1G \
--num-executors 3 \
--executor-cores 2 \
--conf "spark.default.parallelism=6" \
~/bigdata/spark/lib/spark-learning-1.0-SNAPSHOT-jar-with-dependencies.jar
 * 修改脚本可执行权限：[emon@emon ~]$ chmod u+x /home/emon/bigdata/spark/shell/moreParallelismScala.sh
 * 执行脚本：[emon@emon ~]$ sh -x /home/emon/bigdata/spark/shell/moreParallelismScala.sh
 */
object MoreParallelismScala {

  def main(args: Array[String]): Unit = {
    // 第一步：创建SparkContext
    val conf = EnvScalaUtils.buildSparkConfByEnv(this.getClass.getSimpleName)
    // 设置默认并行度为5
    // conf.set("spark.default.parallelism", "5")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Array("hello", "you", "hello", "me", "hehe", "hello", "you", "hello", "me", "hehe"))
    dataRDD.map((_, 1))
      .reduceByKey(_ + _)
      .foreach(println(_))

    // while循环是为了保证程序不结束，方便在本地查看4040页面中的storage信息
    /*while (true) {
      ;
    }*/
    sc.stop()
  }
}
