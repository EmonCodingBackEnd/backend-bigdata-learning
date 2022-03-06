package com.coding.bigdata.flink.batch.transformation;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Arrays;

/*
 * join：内连接
 */
public class BatchJoinJava {

    public static void main(String[] args) throws Exception {
        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 初始化第一份数据 Tuple2<用户id, 用户姓名>
        Tuple2<Integer, String> tup1 = new Tuple2<>(1, "jack");
        Tuple2<Integer, String> tup2 = new Tuple2<>(2, "tom");
        Tuple2<Integer, String> tup3 = new Tuple2<>(3, "mick");
        DataSource<Tuple2<Integer, String>> text1 =
                env.fromCollection(Arrays.asList(tup1, tup2, tup3));
        // 初始化第二份数据 Tuple2<用户id, 用户所在城市>
        Tuple2<Integer, String> tup11 = new Tuple2<>(1, "bj");
        Tuple2<Integer, String> tup22 = new Tuple2<>(2, "sh");
        Tuple2<Integer, String> tup33 = new Tuple2<>(4, "gz");
        DataSource<Tuple2<Integer, String>> text2 =
                env.fromCollection(Arrays.asList(tup11, tup22, tup33));

        // 对两份数据集执行join操作
        text1.join(text2)
                // 注意：这里的where和equalsTo实现类似于on fieldA=fieldB的效果
                // where：指定左边数据集中参与比较的元素角标
                .where(0)
                // equalTo：指定右边数据集中参与比较的元素角标
                .equalTo(0)
                .with(
                        new JoinFunction<
                                Tuple2<Integer, String>,
                                Tuple2<Integer, String>,
                                Tuple3<Integer, String, String>>() {
                            @Override
                            public Tuple3<Integer, String, String> join(
                                    Tuple2<Integer, String> first, Tuple2<Integer, String> second)
                                    throws Exception {
                                return new Tuple3<Integer, String, String>(
                                        first.f0, first.f1, second.f1);
                            }
                        })
                .print();
    }
}
