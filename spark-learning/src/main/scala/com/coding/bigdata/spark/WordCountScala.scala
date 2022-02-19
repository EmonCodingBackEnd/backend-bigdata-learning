package com.coding.bigdata.spark

import com.coding.bigdata.common.EnvScalaUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/*
 * 任务提交方式：
 * 方式1、Window环境执行方式：直接在IDEA中执行，方便在本地环境调试代码
 * 方式2、Linux环境执行方式：使用spark-submit提交到集群执行【实际工作中使用】
 * 创建脚本： [emon@emon ~]$ vim /home/emon/bigdata/spark/shell/wordCountJob.sh
 * 脚本内容：如下：
spark-submit \
--class com.coding.bigdata.spark.WordCountScala \
--master yarn \
--deploy-mode client \
--executor-memory 1G \
--num-executors 1 \
~/bigdata/spark/lib/spark-learning-1.0-SNAPSHOT-jar-with-dependencies.jar \
hdfs://emon:8020/custom/data/spark/hello.txt
 * 修改脚本可执行权限：[emon@emon ~]$ chmod u+x /home/emon/bigdata/spark/shell/wordCountJob.sh
 * 执行脚本：[emon@emon ~]$ sh -x /home/emon/bigdata/spark/shell/wordCountJob.sh
 * 方式3、使用spark-shell，方便在集群环境调试代码
 * spark-shell 或者 spark-shell --master yarn --deploy-mode client
 */
object WordCountScala {

  def main(args: Array[String]): Unit = {
    // 第一步：创建SparkContext
    val conf = EnvScalaUtils.buildSparkConfByEnv(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    // 第二步：加载数据
    /*
     * linesRDD 内容：
     * hello you
     * hello me
     */
    var path = "custom/data/spark/hello.txt"
    if (args.length == 1) {
      path = args(0)
    }
    val linesRDD: RDD[String] = sc.textFile(path)

    // 第三步：对数据进行切割，把一行数据切分
    /*
     * wordsRDD 内容：
     * hello
     * you
     * hello
     * me
     */
    val wordsRDD: RDD[String] = linesRDD.flatMap(_.split(" "))

    // 第四部：迭代words，将每个word转换为 (word,1) 这种形式
    /*
     * pairRDD 内容：
     * (hello,1)
     * (you,1)
     * (hello,1)
     * (me,1)
     */
    val pairRDD: RDD[(String, Int)] = wordsRDD.map((_, 1))

    // 第五步：根据key(其实就是word）进行分组聚合
    /*
     * wordCountRDD 内容：
     * (me,1)
     * (you,1)
     * (hello,2)
     */
    val wordCountRDD: RDD[(String, Int)] = pairRDD.reduceByKey(_ + _)

    // 第六步：将结果打印到控制台
    wordCountRDD.foreach(wordCount => println(wordCount._1 + "--" + wordCount._2))

    // 第七步：停止SparkContext
    sc.stop()

  }

}
