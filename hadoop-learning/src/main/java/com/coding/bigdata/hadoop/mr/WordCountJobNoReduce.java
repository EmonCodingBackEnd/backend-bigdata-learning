package com.coding.bigdata.hadoop.mr;

import com.coding.bigdata.common.EnvUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
 * 只有Map阶段，不包含Reduce阶段
 *
 * Window环境执行方式：直接执行
 * Linux环境执行方式：hadoop jar ~/bigdata/hadoop/lib/hadoop-learning-1.0-SNAPSHOT-jar-with-dependencies.jar com.coding.bigdata.hadoop.mr.WordCountJobNoReduce /custom/data/mr/normal/input/hello.txt /custom/data/mr/normal/output
 */
@Slf4j
public class WordCountJobNoReduce {

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
                String dynamicPrefix = isWinLocal ? "." : "";
                inputPath = dynamicPrefix + "/custom/data/mr/normal/input/hello.txt";
                outputPath = dynamicPrefix + "/custom/data/mr/normal/output";
            } else {
                isWinLocal = false;
                if (args.length != 2) {
                    // 如果传递的参数不够，程序直接退出
                    System.exit(100);
                }
                inputPath = args[0];
                outputPath = args[1];
            }
            EnvUtils.checkInputPath(inputPath, isWinLocal);
            EnvUtils.checkOutputPath(outputPath, isWinLocal);
            // 指定Job需要的配置参数
            Configuration configuration = EnvUtils.buildConfByEnv(isWinLocal);

            // 创建一个Job
            Job job = Job.getInstance(configuration);

            // 注意了：这一行必须设置，否则在集群中执行的时候是找不到WordCountJob这个类的
            job.setJarByClass(WordCountJobNoReduce.class);

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

            // 禁用Reduce
            job.setNumReduceTasks(0);

            // 提交job
            job.waitForCompletion(true);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            log.error(WordCountJobNoReduce.class.getSimpleName(), e);
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
}
