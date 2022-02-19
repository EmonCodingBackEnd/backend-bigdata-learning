package com.coding.bigdata.spark

import com.coding.bigdata.common.EnvScalaUtils
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel


/*
 * 需求：Kryo序列化的使用
 */
object KryoSerScala {

  def main(args: Array[String]): Unit = {
    // 第一步：创建SparkContext
    val conf = EnvScalaUtils.buildSparkConfByEnv(this.getClass.getSimpleName)

    // 第一种：默认序列化，不需要任何额外配置，使用Java序列化
    /*{
    }*/

    // 第二种：指定使用kryo序列化机制，但不注册被序列化的类名。注意：如果使用了registerKryoClasses，其实这一行设置是可以省略的
    /*{
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    }*/

    // 第三种：指定使用kryo序列化机制并注册自定义的数据类型。【推荐】
    {
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注意：如果第二种开启了，该方法非必须，但如果设置了可以节省spark保存类名全路径
      conf.registerKryoClasses(Array(classOf[Person]))
    }

    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Array("hello you", "hello me"))
    val wordsRDD = dataRDD.flatMap(_.split(" "))
    val personRDD = wordsRDD.map(word => Person(word, 18)).persist(StorageLevel.MEMORY_ONLY_SER)
    personRDD.foreach(println(_))

    // while循环是为了保证程序不结束，方便在本地查看4040页面中的storage信息
    while (true) {
      ;
    }
    sc.stop()
  }
}

case class Person(name: String, age: Int) extends Serializable
