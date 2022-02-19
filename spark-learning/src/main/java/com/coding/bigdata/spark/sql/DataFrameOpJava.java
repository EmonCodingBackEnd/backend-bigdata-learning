package com.coding.bigdata.spark.sql;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/*
 * 需求：DataFrame常见操作
 */
public class DataFrameOpJava {

    public static void main(String[] args) {
        // 创建SparkSession对象，里面包含SparkContext和SqlContext
        SparkSession sparkSession =
                EnvUtils.buildSparkSessionByEnv(SqlDemoJava.class.getSimpleName());

        // 读取json文件，获取DataFrame
        Dataset<Row> dataFrame = sparkSession.read().json("custom/data/spark/sql/student.json");

        // 打印schema信息
        dataFrame.printSchema();

        // 默认显示所有数据，可以通过参数控制显示多少条
        dataFrame.show(2);

        // 查询数据中的指定字段信息
        dataFrame.select("name", "age").show();

        // 在使用select的时候可以对数据做一些操作，需要添加隐式转换函数，否则语法报错
        dataFrame.select(col("name"), col("age").plus(1)).show();

        // 对数据进行过滤，需要添加隐式转换函数，否则报错
        dataFrame.filter(col("age").gt(18)).show();
        // where底层调用的就是filter
        dataFrame.where(col("age").gt(18)).show();

        // 对数据进行分组求和
        dataFrame.groupBy("age").count().show();

        // 排序显示
        dataFrame.orderBy("age").show();

        sparkSession.stop();
    }
}
