package com.coding.bigdata.spark;

import com.coding.bigdata.common.EnvUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;
import java.util.Arrays;

/*
 * 需求：Kryo序列化的使用
 */
public class KryoSerJava {

    @SuppressWarnings("all")
    public static void main(String[] args) {
        // 第一步：创建SparkContext
        SparkConf conf = EnvUtils.buildSparkConfByEnv(KryoSerJava.class.getSimpleName());

        // 第一种：默认序列化，不需要任何额外配置，使用Java序列化
        /*{
        }*/

        // 第二种：指定使用kryo序列化机制，但不注册被序列化的类名
        /*{
          conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        }*/

        // 第三种：指定使用kryo序列化机制并注册自定义的数据类型。【推荐】
        {
            conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            conf.set("spark.kryo.classesToRegister", "com.coding.bigdata.spark.Person");
        }

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> dataRDD = sc.parallelize(Arrays.asList("hello you", "hello me"));
        JavaRDD<String> wordsRDD =
                dataRDD.flatMap(
                        (FlatMapFunction<String, String>)
                                line -> Arrays.stream(line.split(" ")).iterator());
        val personRDD =
                wordsRDD.map(word -> new Person2(word, 18)).persist(StorageLevel.MEMORY_ONLY_SER());
        personRDD.foreach(person -> System.out.println(person));

        // while循环是为了保证程序不结束，方便在本地查看4040页面中的storage信息
        while (true) {}
    }
}

@Getter
@Setter
@AllArgsConstructor
class Person2 implements Serializable {
    private static final long serialVersionUID = 134343319470567142L;
    private String name;
    private Integer age;

    @Override
    public String toString() {
        return "Person2{" + "name='" + name + '\'' + ", age=" + age + '}';
    }
}
