package com.coding.bigdata.hadoop.mr.smallfile;

import com.coding.bigdata.common.EnvUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;

/** 小文件解决方案之MapFile */
public class SmallFileMap {

    public static void main(String[] args) throws IOException {
        boolean isWinLocal = false;

        String dynamicPrefix = isWinLocal ? "." : "";
        String inputPath = "./custom/data/mr/smallfile/input/";
        String outputPath = dynamicPrefix + "/custom/data/mr/smallfile/output/map/";

        // 生成MapFile文件
        write(isWinLocal, inputPath, outputPath);

        // 读取MapFile文件
        read(isWinLocal, outputPath);
    }

    /**
     * 生成SequenceFile文件
     *
     * @param isWinLocal - 是否在Win本地环境处理
     * @param inputPath - 输入目录-windows目录
     * @param outputPath - 输出文件-hdfs文件
     * @throws IOException -
     */
    private static void write(boolean isWinLocal, String inputPath, String outputPath)
            throws IOException {
        EnvUtils.checkInputPath(inputPath, true);
        EnvUtils.checkOutputPath(outputPath, isWinLocal);

        // 创建一个配置对象
        Configuration configuration = EnvUtils.buildConfByEnv(isWinLocal);
        // 构造 options 数组，有两个元素
        /*
        第二个是key的类型
        第三个是value的类型
         */
        SequenceFile.Writer.Option[] options = {
            MapFile.Writer.keyClass(Text.class), MapFile.Writer.valueClass(Text.class)
        };
        // 创建了一个writer示例
        MapFile.Writer writer = new MapFile.Writer(configuration, new Path(outputPath), options);

        // 指定需要压缩的文件目录
        File inputFile = new File(inputPath);
        if (inputFile.isDirectory()) {
            // 获取目录中的文件
            File[] files = inputFile.listFiles();
            if (files != null) {
                for (File file : files) {
                    // 获取文件的全部内容
                    String content = FileUtils.readFileToString(file, "UTF-8");
                    // 获取文件名
                    String fileName = file.getName();
                    Text key = new Text(fileName);
                    Text value = new Text(content);
                    // 向SequenceFile中写入数据
                    writer.append(key, value);
                }
            }
        }
        writer.close();
    }

    /**
     * 读取SequenceFile文件
     *
     * @param isWinLocal - 是否在Win本地环境处理
     * @param inputPath - SequenceFile路径
     * @throws IOException
     */
    private static void read(boolean isWinLocal, String inputPath) throws IOException {
        EnvUtils.checkInputPath(inputPath, isWinLocal);

        // 创建一个配置对象
        Configuration configuration = EnvUtils.buildConfByEnv(isWinLocal);
        // 构造 options 数组，有三个元素

        MapFile.Reader reader = new MapFile.Reader(new Path(inputPath), configuration);
        Text key = new Text();
        Text value = new Text();

        // 循环读取数据
        while (reader.next(key, value)) {
            System.out.print("key = " + key);
            System.out.println(" value " + value);
        }
        reader.close();
    }
}
