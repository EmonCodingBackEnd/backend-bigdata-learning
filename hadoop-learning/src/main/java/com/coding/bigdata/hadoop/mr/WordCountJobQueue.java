package com.coding.bigdata.hadoop.mr;

import com.coding.bigdata.common.EnvUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/*
 * 需求：指定队列名称
 *
 * Window环境执行方式：直接执行
 * Linux环境执行方式：hadoop jar ~/bigdata/hadoop/lib/hadoop-learning-1.0-SNAPSHOT-jar-with-dependencies.jar com.coding.bigdata.hadoop.mr.WordCountJobQueue -Dmapreduce.job.queuename=default /custom/data/mr/normal/input/hello.txt /custom/data/mr/normal/output
 */
@Slf4j
public class WordCountJobQueue {

    /**
     * 组装Job
     *
     * @param args - 输入输出路径<br>
     *     比如： /custom/data/mr/normal/input/hello.txt 和 /custom/data/mr/normal/output
     */
    public static void main(String[] args) {
        boolean isWinLocal;
        try {
            String inputPath;
            String outputPath;
            if (EnvUtils.isWin) {
                isWinLocal = true;
                if (args.length == 0) {
                    args = new String[3];
                    args[0] = "-Dmapreduce.job.queuename=offline";
                    String dynamicPrefix = isWinLocal ? "." : "";
                    args[1] = dynamicPrefix + "/custom/data/mr/normal/input/hello.txt";
                    args[2] = dynamicPrefix + "/custom/data/mr/normal/output";
                }
            } else {
                isWinLocal = false;
            }

            // 指定Job需要的配置参数
            Configuration configuration = EnvUtils.buildConfByEnv(isWinLocal);
            // 解析命令行中通过-D传递过来的参数，添加到conf中
            String[] remainingArgs =
                    new GenericOptionsParser(configuration, args).getRemainingArgs();
            if (remainingArgs.length != 2) {
                // 如果传递的参数不够，程序直接退出
                log.error("传递的参数不够，程序直接退出");
                System.exit(100);
            }
            inputPath = remainingArgs[0];
            outputPath = remainingArgs[1];

            EnvUtils.checkInputPath(inputPath, isWinLocal);
            EnvUtils.checkOutputPath(outputPath, isWinLocal);

            // 创建一个Job
            Job job = Job.getInstance(configuration);

            // 注意了：这一行必须设置，否则在集群中执行的时候是找不到WordCountJob这个类的
            job.setJarByClass(WordCountJobQueue.class);

            // 指定输入路径（可以是文件，也可以是目录）
            FileInputFormat.setInputPaths(job, new Path(inputPath));
            // 指定输出路径（只能指定一个不存在的目录）
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            // 指定map相关代码
            job.setMapperClass(MyMapper.class);
            // 指定k2的类型
            job.setMapOutputKeyClass(Text.class);
            // 指定v2的类型
            job.setMapOutputValueClass(LongWritable.class);

            // 指定reduce相关代码
            job.setReducerClass(MyReducer.class);
            // 指定k3的类型
            job.setOutputKeyClass(Text.class);
            // 指定v3的类型
            job.setOutputValueClass(LongWritable.class);

            // 提交job
            job.waitForCompletion(true);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            log.error(WordCountJobQueue.class.getSimpleName(), e);
        }
    }

    /** Map阶段 */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        /**
         * @param key - 每一行数据的行首偏移量
         * @param value - 一行数据
         * @param context - 上下文
         * @throws IOException -
         * @throws InterruptedException -
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word : words) {
                Text key2 = new Text(word);
                LongWritable value2 = new LongWritable(1L);
                context.write(key2, value2);
            }
        }
    }

    /** Reduce阶段 */
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        /**
         * 针对<k2, {v2......}>的数据进行累加求和，并且最终把数据转换为<k3,v3>写出去
         *
         * @param key -
         * @param values -
         * @param context -
         * @throws IOException -
         * @throws InterruptedException -
         */
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0L;
            for (LongWritable value : values) {
                sum += value.get();
            }

            Text key3 = key;
            LongWritable value3 = new LongWritable(sum);
            context.write(key3, value3);
        }
    }
}
