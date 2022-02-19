package com.coding.bigdata.spark.sql;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/*
 * 需求：load和save的使用
 */
public class LoadAndSaveOpJava {

    public static void main(String[] args) {
        // 创建SparkSession对象，里面包含SparkContext和SqlContext
        SparkSession sparkSession =
                EnvUtils.buildSparkSessionByEnv(SqlDemoJava.class.getSimpleName());

        // 读取json文件，获取Dataset
        Dataset<Row> stuDF =
                sparkSession.read().format("json").load("custom/data/spark/sql/student.json");


        // 保存数据
        boolean isWinLocal = true;
        String path = "custom/data/spark/sql/output";
        if (!EnvUtils.isWin) {
            path = EnvUtils.toHDFSPath(path);
        }
        // 指定HDFS的路径信息即可，需要指定一个不存在的目录
        EnvUtils.checkOutputPath(path, isWinLocal);
        stuDF.select("name", "age").write().format("csv").save(path);

        sparkSession.stop();
    }
}
