package com.coding.bigdata.spark;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/*
 * 需求：checkpoint的使用
 */
public class CheckpointOpJava {

    public static void main(String[] args) {
        // 第一步：创建SparkContext
        SparkConf conf = EnvUtils.buildSparkConfByEnv(WordCountJava.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        boolean isWinLocal = true;
        String checkpointDir = "checkpoint/chk001";
        String filePath = "custom/data/mr/skew/input/hello_10000000.dat";
        String outputPath = "custom/data/spark/chk001";
        if (!EnvUtils.isWin) {
            isWinLocal = false;
            checkpointDir = EnvUtils.toHDFSPath(checkpointDir);
            filePath = EnvUtils.toHDFSPath(filePath);
            outputPath = EnvUtils.toHDFSPath(outputPath);
        }
        // 清理checkpoint目录：非必须
        EnvUtils.checkOutputPath(checkpointDir, isWinLocal);
        // 指定HDFS的路径信息即可，需要指定一个不存在的目录
        EnvUtils.checkOutputPath(outputPath, isWinLocal);

        // 1、设置checkpoint目录
        JavaRDD<String> dataRDD = sc.textFile(filePath);
        sc.setCheckpointDir(checkpointDir);

        // 2、对RDD执行checkpoint操作
        dataRDD.checkpoint();

        dataRDD.flatMap(
                        (FlatMapFunction<String, String>)
                                line -> Arrays.stream(line.split(" ")).iterator())
                .mapToPair((PairFunction<String, String, Integer>) line -> new Tuple2<>(line, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum)
                .saveAsTextFile(outputPath);

        sc.stop();
    }
}
