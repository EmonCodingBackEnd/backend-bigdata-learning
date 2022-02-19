package com.coding.bigdata.spark.sql;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/*
 * 需求：使用sql操作DataFrame
 */
public class DataFrameSqlJava {

    public static void main(String[] args) {
        // 创建SparkSession对象，里面包含SparkContext和SqlContext
        SparkSession sparkSession =
                EnvUtils.buildSparkSessionByEnv(SqlDemoJava.class.getSimpleName());

        // 读取json文件，获取DataFrame
        Dataset<Row> dataFrame = sparkSession.read().json("custom/data/spark/sql/student.json");

        // 将DataFrame注册为一个临时表
        dataFrame.createOrReplaceTempView("student");

        // 使用sql查询临时表中的数据
        sparkSession.sql("select age,count(*) as num from student group by age").show();

        sparkSession.stop();
    }
}
