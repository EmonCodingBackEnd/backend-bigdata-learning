package com.coding.bigdata.spark.sql;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
 * 需求：通过反射方式实现RDD转换为DataFrame
 */
public class RDDToDataFrameByProgramJava {

    public static void main(String[] args) {
        // 创建SparkSession对象，里面包含SparkContext和SqlContext
        SparkSession sparkSession =
                EnvUtils.buildSparkSessionByEnv(SqlDemoJava.class.getSimpleName());

        // 获取SparkContext
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        Tuple2<String, Integer> t1 = new Tuple2<>("jack", 18);
        Tuple2<String, Integer> t2 = new Tuple2<>("tom", 20);
        Tuple2<String, Integer> t3 = new Tuple2<>("jessic", 30);
        JavaRDD<Tuple2<String, Integer>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3));

        // 组装rowRDD
        JavaRDD<Row> rowRDD = dataRDD.map(tup -> RowFactory.create(tup._1, tup._2));

        // 指定元数据信息【这个元数据信息就可以动态从外部获取了，比较灵活】
        ArrayList<StructField> structFieldList = new ArrayList<>();
        structFieldList.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType schema = DataTypes.createStructType(structFieldList);

        // 组装DataFrame
        Dataset<Row> stuDF = sparkSession.createDataFrame(rowRDD, schema);

        // 下面就可以通过DataFrame的方式操作dataRDD中的数据了
        stuDF.createOrReplaceTempView("student");

        // 执行sql查询
        Dataset<Row> resDF = sparkSession.sql("select name, age from student where age > 18");

        // 将DataFrame转化为RDD
        JavaRDD<Row> resRDD = resDF.javaRDD();

        // 从row中取数据，封装成student，打印到控制台
        List<Tuple2<String, Integer>> resList =
                resRDD.map(row -> new Tuple2<>(row.getString(0), row.getInt(1))).collect();
        for (Tuple2 tup : resList) {
            System.out.println(tup);
        }

        // 使用row的getAs方法，获取指定列名的值
        List<Tuple2<String, Integer>> resList2 =
                resRDD.map(
                                row ->
                                        new Tuple2<>(
                                                row.getAs("name").toString(),
                                                Integer.parseInt(row.getAs("age").toString())))
                        .collect();
        for (Tuple2 tup : resList2) {
            System.out.println(tup);
        }
    }
}
