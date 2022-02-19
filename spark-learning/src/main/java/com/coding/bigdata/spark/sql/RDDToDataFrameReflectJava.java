package com.coding.bigdata.spark.sql;

import com.coding.bigdata.common.EnvUtils;
import com.coding.bigdata.spark.sql.domain.Student;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/*
 * 需求：通过反射方式实现RDD转换为DataFrame
 */
public class RDDToDataFrameReflectJava {

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

        // 基于反射直接将包含Student对象的dataRDD转换为dataFrame
        // 注意：Student这个类必须声明为public，并且必须实现序列化
        JavaRDD<Student> stuRDD = dataRDD.map(tup -> new Student(tup._1, tup._2));
        Dataset<Row> stuDF = sparkSession.createDataFrame(stuRDD, Student.class);

        // 下面就可以通过DataFrame的方式操作dataRDD中的数据了
        stuDF.createOrReplaceTempView("student");

        // 执行sql查询
        Dataset<Row> resDF = sparkSession.sql("select name, age from student where age > 18");

        // 将DataFrame转化为RDD
        JavaRDD<Row> resRDD = resDF.javaRDD();

        // 从row中取数据，封装成student，打印到控制台
        List<Student> resList =
                resRDD.map(row -> new Student(row.getString(0), row.getInt(1))).collect();
        for (Student student : resList) {
            System.out.println(student);
        }

        // 使用row的getAs方法，获取指定列名的值
        List<Student> resList2 =
                resRDD.map(
                                row ->
                                        new Student(
                                                row.getAs("name").toString(),
                                                Integer.parseInt(row.getAs("age").toString())))
                        .collect();
        for (Student student : resList2) {
            System.out.println(student);
        }
    }
}
