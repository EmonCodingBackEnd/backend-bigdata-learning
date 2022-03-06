package com.coding.bigdata.flink.tablesql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/*
 * 将DataStream转换为表
 */
public class DataStreamToTableJava {

    public static void main(String[] args) {
        // 获取 StreamTableEnvironment
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings streamSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnv =
                StreamTableEnvironment.create(streamEnv, streamSettings);

        // 获取DataStream
        Tuple2<Integer, String> tup1 = new Tuple2<>(1, "jack");
        Tuple2<Integer, String> tup2 = new Tuple2<>(2, "tom");
        Tuple2<Integer, String> tup3 = new Tuple2<>(3, "mack");
        DataStreamSource<Tuple2<Integer, String>> stream =
                streamEnv.fromCollection(Arrays.asList(tup1, tup2, tup3));

        // 第一种：将DataStream转换为view视图
        streamTableEnv.createTemporaryView("myTable", stream, $("id"), $("name"));
        streamTableEnv.sqlQuery("select * from myTable where id > 1").execute().print();

        // 第二种：将DataStream转换为Table对象
        Table table = streamTableEnv.fromDataStream(stream, $("id"), $("name"));
        table.select($("id"), $("name")).filter($("id").isGreater(1)).execute().print();
    }
}
