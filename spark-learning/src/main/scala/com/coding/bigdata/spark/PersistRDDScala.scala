package com.coding.bigdata.spark

import com.coding.bigdata.common.EnvScalaUtils
import org.apache.spark.SparkContext

/*
 * 需求：RDD持久化
 *
 * 如何设置IDEA显示内存使用情况？
 * View=>Appearance=>Status Bar Widgets=>Memory Indicator【勾上】
 * 如何设置IDEA的内存？
 * Help=>Edit Custom VM Options...【点击】
 *
 * RDD持久化原理：
 * Spark中有一个非常重要的功能就是可以对RDD进行持久化。
 * 当对RDD执行持久化操作时，每个节点都会讲自己操作的RDD的partition数据持久化到内存中，
 * 并且在之后对该RDD的反复使用中，直接使用内存中缓存的partition数据。
 *
 * 这样的话，针对一个RDD反复执行多个操作的场景，就只需要对RDD计算一次即可，后面直接使用该RDD，
 * 而不需要反复计算多次该RDD。
 *
 * 正常情况下RDD的数据使用过后是不会一直保存的，巧妙使用RDD持久化，在某些场景下，对Spark应用程序的性能有很大提升。
 * 特别是对于迭代式算法和快速交互式应用来说，RDD持久化，是非常重要的。
 *
 * 要持久化一个RDD，只需要调用它的 cache() 或者 persist() 方法就可以了。
 * 在该RDD第一次被计算出来时，就会直接缓存在每个节点中。而且Spark的持久化机制还是自动容错的，
 * 如果持久化的RDD的任何partition数据丢失了，那么Spark会自动通过其源RDD，使用transformation算子重新计算该partition的数据。
 *
 * cache()和persist()的区别在于：
 * cache()是persist()的一种简化方式:  cache() = persist() = persist(StorageLevel.MEMORY_ONLY)
 *
 * RDD持久化策略：
 * 策略                 介绍
 * MEMORY_ONLY          以非序列化的方式持久化在JVM内存中
 * MEMORY_AND_DISK      同上，但是当某些partition无法存储在内存中时，会持久化到磁盘中
 * MEMORY_ONLY_SER      同MEMORY_ONLY，但是会序列化
 * MEMORY_AND_DISK_SET  同MEMORY_AND_DISK，但是会序列化
 * DISK_ONLY            以非序列化的方式完全存储到磁盘上
 * MEMORY_ONLY_2、MEMORY_AND_DISK_2等，尾部加了2的持久化级别，表示会将持久化数据复制一份，保存到其他节点，
 * 从而在数据丢失时，不需要重新计算，只需要使用备份数据即可。
 */
object PersistRDDScala {

  def main(args: Array[String]): Unit = {
    // 第一步：创建SparkContext
    val conf = EnvScalaUtils.buildSparkConfByEnv(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val path = "custom/data/mr/skew/input/hello_10000000.dat"
    // 注意cache的用法和位置，cache默认是基于内存的持久化
    val dataRDD = sc.textFile(path).cache()


    var startTime = System.currentTimeMillis()
    var count = dataRDD.count()
    println(count)
    var endTime = System.currentTimeMillis()
    println("第一次耗时：" + (endTime - startTime))

    /*
    // 如果发现启用了缓存后，第二次耗时仍旧未减少，请查看日志，是否： Not enough space to cache rdd_1_39 in memory!
    如何解决？待解决？
     */
    // TODO: Not enough space to cache rdd_1_39 in memory!
    startTime = System.currentTimeMillis()
    count = dataRDD.count()
    println(count)
    endTime = System.currentTimeMillis()
    println("第二次耗时：" + (endTime - startTime))

    sc.stop()
  }
}
