package com.coding.bigdata.hadoop.mr.smallfile;

import com.coding.bigdata.common.EnvUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;

/** 小文件解决方案之SequenceFile */
public class SmallFileSeq {

    public static void main(String[] args) throws IOException {
        boolean isWinLocal = false;

        String dynamicPrefix = isWinLocal ? "." : "";
        String inputPath = "./custom/data/mr/smallfile/input/";
        String outputFile = dynamicPrefix + "/custom/data/mr/smallfile/output/seq/seqFile";

        // 生成SequenceFile文件
        write(isWinLocal, inputPath, outputFile);

        // 读取SequenceFile文件
        read(isWinLocal, outputFile);
    }

    /**
     * 生成SequenceFile文件
     *
     * @param isWinLocal - 是否在Win本地环境处理
     * @param inputPath - 输入目录-windows目录
     * @param outputFile - 输出文件-hdfs文件
     * @throws IOException -
     */
    private static void write(boolean isWinLocal, String inputPath, String outputFile)
            throws IOException {
        EnvUtils.checkInputPath(inputPath, true);
        EnvUtils.checkOutputPath(outputFile, isWinLocal);

        // 创建一个配置对象
        Configuration configuration = EnvUtils.buildConfByEnv(isWinLocal);
        // 构造 options 数组，有三个元素
        /*
        第一个是输出路径【文件】
        第二个是key的类型
        第三个是value的类型
         */
        SequenceFile.Writer.Option[] options = {
            SequenceFile.Writer.file(new Path(outputFile)),
            SequenceFile.Writer.keyClass(Text.class),
            SequenceFile.Writer.valueClass(Text.class)
        };
        // 创建了一个writer示例
        SequenceFile.Writer writer = SequenceFile.createWriter(configuration, options);

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
     * @param inputFile - SequenceFile路径
     * @throws IOException
     */
    private static void read(boolean isWinLocal, String inputFile) throws IOException {
        EnvUtils.checkInputPath(inputFile, isWinLocal);

        // 创建一个配置对象
        Configuration configuration = EnvUtils.buildConfByEnv(isWinLocal);
        // 构造 options 数组，有三个元素

        SequenceFile.Reader reader =
                new SequenceFile.Reader(
                        configuration, SequenceFile.Reader.file(new Path(inputFile)));
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
