package com.coding.bigdata.flink.batch.transformation;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.Arrays;

/*
 * cross：获取两个数据集的笛卡尔积
 */
public class BatchCrossJava {

    public static void main(String[] args) throws Exception {
        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 初始化第一份数据
        DataSource<Integer> text1 = env.fromCollection(Arrays.asList(1, 2));
        // 初始化第二份数据
        DataSource<String> text2 = env.fromCollection(Arrays.asList("a", "b"));

        text1.cross(text2).print();
    }
}
