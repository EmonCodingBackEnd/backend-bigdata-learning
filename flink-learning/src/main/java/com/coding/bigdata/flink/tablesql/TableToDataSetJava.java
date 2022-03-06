package com.coding.bigdata.flink.tablesql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/*
 * 将table转换成DataSet
 */
public class TableToDataSetJava {

    public static void main(String[] args) throws Exception {
        // 获取 TableEnvironment
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        // 注意：此时只能使用旧的执行引擎，新的Blink执行引擎不支持和DataSet转换
        BatchTableEnvironment batchTableEnv = BatchTableEnvironment.create(batchEnv);

        // 创建输入表
        batchTableEnv.executeSql(
                "create table myTable(\n"
                        + "id int,\n"
                        + "name string\n"
                        + ") with (\n"
                        + "'connector.type' = 'filesystem',\n"
                        + "'connector.path' = 'custom/data/flink/source/input',\n"
                        + "'format.type' = 'csv'\n"
                        + ")");

        // 获取table
        Table table = batchTableEnv.from("myTable");

        // 将Table转换成DataSet
        DataSet<Row> set = batchTableEnv.toDataSet(table, Row.class);
        set.map(new MapFunction<Row, Tuple2<Integer,String>>() {
            @Override
            public Tuple2<Integer, String> map(Row row) throws Exception {
                Integer id =Integer.parseInt( row.getField(0).toString());
                String name = row.getField(1).toString();
                return new Tuple2<>(id,name);
            }
        }).print();


    }
}
