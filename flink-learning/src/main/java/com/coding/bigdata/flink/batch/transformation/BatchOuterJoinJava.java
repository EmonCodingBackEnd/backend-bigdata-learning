package com.coding.bigdata.flink.batch.transformation;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Arrays;

/*
 * outerJoin：外连接
 * 一共有三种情况
 * 1：leftOuterJoin
 * 2：rightOuterJoin
 * 3：fullOuterJoin
 */
public class BatchOuterJoinJava {

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

        // 对两份数据集执行 leftOuterJoin 操作
        text1.leftOuterJoin(text2)
                .where(0)
                .equalTo(0)
                /*
                 * 【提醒】千万千万不要根据提示简化代码，否则会报错：
                 * The generic type parameters of 'Tuple3' are missing. In many cases lambda methods don't provide enough information for automatic type extraction when Java generics are involved. An easy workaround is to use an (anonymous) class instead that implements the 'org.apache.flink.api.common.functions.JoinFunction' interface. Otherwise the type has to be specified explicitly using type information.
                 */
                .with(
                        new JoinFunction<
                                Tuple2<Integer, String>,
                                Tuple2<Integer, String>,
                                Tuple3<Integer, String, String>>() {
                            @Override
                            public Tuple3<Integer, String, String> join(
                                    Tuple2<Integer, String> first, Tuple2<Integer, String> second)
                                    throws Exception {
                                // 注意：second中的元素可能为null
                                if (second == null) {
                                    return new Tuple3<>(first.f0, first.f1, "null");
                                } else {
                                    return new Tuple3<>(first.f0, first.f1, second.f1);
                                }
                            }
                        })
                .print();

        System.out.println("==================================================");

        // 对两份数据集执行 rightOuterJoin 操作
        text1.rightOuterJoin(text2)
                .where(0)
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
                                // 注意：first中的元素可能为null
                                if (first == null) {
                                    return new Tuple3<>(second.f0, "null", second.f1);
                                } else {
                                    return new Tuple3<>(first.f0, first.f1, second.f1);
                                }
                            }
                        })
                .print();

        System.out.println("==================================================");

        // 对两份数据集执行 fullOuterJoin 操作
        text1.fullOuterJoin(text2)
                .where(0)
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
                                // 注意：first中的元素可能为null
                                if (first == null) {
                                    return new Tuple3<>(second.f0, "null", second.f1);
                                }
                                // 注意：second中的元素可能为null
                                else if (second == null) {
                                    return new Tuple3<>(first.f0, first.f1, "null");
                                } else {
                                    return new Tuple3<>(first.f0, first.f1, second.f1);
                                }
                            }
                        })
                .print();
    }
}
