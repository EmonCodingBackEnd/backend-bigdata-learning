package com.coding.bigdata.flink.batch.transformation;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;

/*
 * MapPartition的使用：一次处理一个分区的数据
 */
public class BatchMapPartitionJava {

    public static void main(String[] args) throws Exception {
        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 生成数据源数据
        DataSource<String> text = env.fromCollection(Arrays.asList("hello you", "hello me"));

        // 每次处理一个分区的数据
        text.mapPartition(
                        new MapPartitionFunction<String, String>() {
                            @Override
                            public void mapPartition(
                                    Iterable<String> iterable, Collector<String> collector)
                                    throws Exception {
                                // 可以在此处创建数据库连接，建议把这块代码放到try-cache代码中
                                // 注意：此时是每个分区获取一次数据库连接，不需要每处理一条数据就获取一次连接，性能较高
                                Iterator<String> it = iterable.iterator();
                                while (it.hasNext()) {
                                    String line = it.next();
                                    String[] words = line.split(" ");
                                    for (String word : words) {
                                        collector.collect(word);
                                    }
                                    // 关闭数据库连接
                                }
                            }
                        })
                .print();

        // No new data sinks have been defined since the last execution. The last execution refers
        // to the latest call to 'execute()', 'count()', 'collect()', or 'print()'.
        // 注意：针对DataSetAPI，如果在后面调用的是count、collect、print，则最后不需要指定execute即可。
        // env.execute(BatchMapPartitionJava.class.getSimpleName());
    }
}
