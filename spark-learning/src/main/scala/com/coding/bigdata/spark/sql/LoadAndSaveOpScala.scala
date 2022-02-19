package com.coding.bigdata.spark.sql

import com.coding.bigdata.common.{EnvScalaUtils, EnvUtils}
import org.apache.spark.sql.SaveMode


/*
 * 需求：load和save的使用
 */
object LoadAndSaveOpScala {

  def main(args: Array[String]): Unit = {
    // 创建SparkSession对象，里面包含SparkContext和SqlContext
    val sparkSession = EnvScalaUtils.buildSparkSessionByEnv(this.getClass.getSimpleName)

    // 读取数据
    val stuDF = sparkSession.read.format("json").load("custom/data/spark/sql/student.json")

    // 保存数据
    val isWinLocal = true
    var path = "custom/data/spark/sql/output"
    if (!EnvUtils.isWin) {
      path = EnvUtils.toHDFSPath(path)
    }
    // 指定HDFS的路径信息即可，需要指定一个不存在的目录
//    EnvUtils.checkOutputPath(path, isWinLocal)
    stuDF.select("name", "age").write.format("csv").mode(SaveMode.Append).save(path)

    sparkSession.stop()
  }
}

/*
 * Spark SQL对于save操作，提供了不同的save mode。
 * SaveMode                       解释
 * SaveMode.ErrorIfExists(默认)   如果目标位置已经存在数据，那么抛出一个异常
 * SaveMode.Append               如果目标位置已经存在数据，那么将数据追加进去
 * SaveMode.Overwrite            如果目标位置已经存在数据，那么就将已经存在的数据删除
 * SaveMode.Ignore               如果目标位置已经存在数据，那么就忽略，不做任何操作
 */