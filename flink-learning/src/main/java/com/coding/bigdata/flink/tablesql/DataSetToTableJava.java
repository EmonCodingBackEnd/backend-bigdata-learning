package com.coding.bigdata.flink.tablesql;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/*
 * 将DataSet转换为表
 */
public class DataSetToTableJava {

    public static void main(String[] args) {
        // 获取 BatchTableEnvironment
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        // 注意：此时只能使用旧的执行引擎，新的Blink执行引擎不支持和DataSet转换
        BatchTableEnvironment batchTableEnv = BatchTableEnvironment.create(batchEnv);

        // 获取DataSet
        Tuple2<Integer, String> tup1 = new Tuple2<>(1, "jack");
        Tuple2<Integer, String> tup2 = new Tuple2<>(2, "tom");
        Tuple2<Integer, String> tup3 = new Tuple2<>(3, "mack");
        DataSource<Tuple2<Integer, String>> set =
                batchEnv.fromCollection(Arrays.asList(tup1, tup2, tup3));

        // 第一种：将DataSet转换为view视图
        batchTableEnv.createTemporaryView("myTable", set, $("id"), $("name"));
        batchTableEnv.sqlQuery("select * from myTable where id > 1").execute().print();

        // 第二种：将DataSet转换为table对象1
        Table table = batchTableEnv.fromDataSet(set, $("id"), $("name"));
        table.select($("id"), $("name")).filter($("id").isGreater(1)).execute().print();
    }
}
