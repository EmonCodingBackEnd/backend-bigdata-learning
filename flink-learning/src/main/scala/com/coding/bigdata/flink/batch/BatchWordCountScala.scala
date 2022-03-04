package com.coding.bigdata.flink.batch

import com.coding.bigdata.common.EnvUtils
import org.apache.flink.api.scala.ExecutionEnvironment

/*
 * 需求：统计指定文件中单词出现的总次数
 */
object BatchWordCountScala {

  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    var isWinLocal = true
    var inputPath = "custom/data/flink/batch/input/hello.txt"
    var outputPath = "custom/data/flink/batch/output"
    if (!EnvUtils.isWin || !isWinLocal) {
      isWinLocal = false
      inputPath = EnvUtils.toHDFSPath(inputPath)
      outputPath = EnvUtils.toHDFSPath(outputPath)
    }
    EnvUtils.checkInputPath(inputPath, isWinLocal)
    EnvUtils.checkOutputPath(outputPath, isWinLocal)

    // 读取文件中的数据
    val text = env.readTextFile(inputPath)

    // 处理数据
    // 注意：必须要添加这一行隐式转换的代码，否则下面的flatMap方法会报错
    import org.apache.flink.api.scala._
    val wordCount = text.flatMap(_.split(" ")) // 将每一行数据根据空格切分单词
      .map((_, 1)) // 每一个单词转换为tuple2的形式（单词，1）
      .groupBy(0)
      .sum(1)
      .setParallelism(1) // 这里面设置并行度为1是为了将所有数据写到一个文件里面，查看结果比较方便；如果是1个并行度，outputPath会按照文件处理，否则按照目录处理

    // 将结果数据保存到文件中
    wordCount.writeAsCsv(outputPath, "\n", " ")

    // 执行程序
    env.execute(this.getClass.getSimpleName)
  }
}
