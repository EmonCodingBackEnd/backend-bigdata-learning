package com.coding.bigdata.flink.tablesql;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*
 * 创建TableEnvironment对象
 */
public class CreateTableEnvironmentJava {

    public static void main(String[] args) {
        /*
         * 注意：如果Table API和SQL不需要和DataStream或者DataSet互相转换
         * 则针对stream和batch都可以使用TableEnvironment
         */
        // 指定底层引擎为Blink，以及数据处理模式-stream
        EnvironmentSettings streamSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        // 创建TableEnvironment对象
        TableEnvironment streamTableEnv = TableEnvironment.create(streamSettings);

        // 指定底层引擎为Blink，以及数据处理模式-batch
        EnvironmentSettings batchSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        // 创建TableEnvironment对象
        TableEnvironment batchTableEnv = TableEnvironment.create(batchSettings);

        /*
         * 注意：如果Table API和SQL需要和DataStream或者DataSet互相转换
         * 则针对stream需要使用 StreamTableEnvironment
         * 针对batch需要使用 BatchTableEnvironment
         */
        // 创建 StreamTableEnvironment
        StreamExecutionEnvironment streamEnv2 =
                StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings streamSettings2 =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnv2 =
                StreamTableEnvironment.create(streamEnv2, streamSettings2);

        // 创建 BatchTableEnvironment
        ExecutionEnvironment batchEnv2 = ExecutionEnvironment.getExecutionEnvironment();
        // 注意：此时只能使用旧的执行引擎，新的Blink执行引擎不支持和DataSet转换
        BatchTableEnvironment batchTableEnv2 = BatchTableEnvironment.create(batchEnv2);
    }
}
