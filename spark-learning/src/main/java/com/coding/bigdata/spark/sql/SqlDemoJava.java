package com.coding.bigdata.spark.sql;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/*
 * 需求：使用json文件创建DataFrame
 */
public class SqlDemoJava {

    public static void main(String[] args) {
        // 创建SparkSession对象，里面包含SparkContext和SqlContext
        SparkSession sparkSession =
                EnvUtils.buildSparkSessionByEnv(SqlDemoJava.class.getSimpleName());

        // 读取json文件，获取Dataset
        Dataset<Row> dataset = sparkSession.read().json("custom/data/spark/sql/student.json");
        // DataFrame和DataSet可以互换
        Dataset<Row> rowDataset = dataset.toDF();

        // 查看Dataset中的数据
        dataset.show();
    }
}
