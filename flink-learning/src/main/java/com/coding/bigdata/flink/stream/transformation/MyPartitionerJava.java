package com.coding.bigdata.flink.stream.transformation;

import org.apache.flink.api.common.functions.Partitioner;

/*
 * 自定义分区规则：按照数字的奇偶性进行分区
 */
public class MyPartitionerJava implements Partitioner<Integer> {
    private static final long serialVersionUID = 8844358521673933825L;

    @Override
    public int partition(Integer key, int numPartitions) {
        System.out.println("分区总数：" + numPartitions);
        if (key % 2 == 0) { // 偶数分到0号分区
            return 0;
        } else { // 奇数分到1号分区
            return 1;
        }
    }
}
