package com.coding.bigdata.hadoop.hdfs;

import com.coding.bigdata.common.EnvUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Java代码操作HDFS.
 *
 * <p>创建时间: <font style="color:#00FFFF">20220121 09:48</font><br>
 * [请在此输入功能详述]
 *
 * @author emon
 * @version 1.0.0
 * @since 1.0.0
 */
public class HdfsCommand {
    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = EnvUtils.buildFileSystemByEnv(false);

        uploadNormal(fileSystem);
        uploadMrBlock1000W(fileSystem);
        uploadSparkHello(fileSystem);
        uploadSparkVideoInfo(fileSystem);
        uploadSparkGiftRecord(fileSystem);
    }

    private static void uploadNormal(FileSystem fileSystem) throws IOException {
        FileInputStream fis = new FileInputStream("./custom/data/mr/normal/input/hello.txt");
        FSDataOutputStream fsdos =
                fileSystem.create(new Path("/custom/data/mr/normal/input/hello.txt"));
        // 上传文件：通过工具类把输入流拷贝到输出流里面，实现本地文件上传到HDFS
        IOUtils.copyBytes(fis, fsdos, 1024, true);
    }

    private static void uploadMrBlock1000W(FileSystem fileSystem) throws IOException {
        FileInputStream fis = new FileInputStream("./custom/data/mr/skew/input/hello_10000000.dat");
        FSDataOutputStream fsdos =
                fileSystem.create(new Path("/custom/data/mr/skew/input/hello_10000000.dat"));
        // 上传文件：通过工具类把输入流拷贝到输出流里面，实现本地文件上传到HDFS
        IOUtils.copyBytes(fis, fsdos, 1024, true);
    }

    private static void uploadSparkHello(FileSystem fileSystem) throws IOException {
        FileInputStream fis = new FileInputStream("./custom/data/spark/hello.txt");
        FSDataOutputStream fsdos = fileSystem.create(new Path("/custom/data/spark/hello.txt"));
        // 上传文件：通过工具类把输入流拷贝到输出流里面，实现本地文件上传到HDFS
        IOUtils.copyBytes(fis, fsdos, 1024, true);
    }

    private static void uploadSparkVideoInfo(FileSystem fileSystem) throws IOException {
        FileInputStream fis = new FileInputStream("./custom/data/spark/topN/input/video_info.log");
        FSDataOutputStream fsdos =
                fileSystem.create(new Path("/custom/data/spark/topN/input/video_info.log"));
        // 上传文件：通过工具类把输入流拷贝到输出流里面，实现本地文件上传到HDFS
        IOUtils.copyBytes(fis, fsdos, 1024, true);
    }

    private static void uploadSparkGiftRecord(FileSystem fileSystem) throws IOException {
        FileInputStream fis = new FileInputStream("./custom/data/spark/topN/input/gift_record.log");
        FSDataOutputStream fsdos =
                fileSystem.create(new Path("/custom/data/spark/topN/input/gift_record.log"));
        // 上传文件：通过工具类把输入流拷贝到输出流里面，实现本地文件上传到HDFS
        IOUtils.copyBytes(fis, fsdos, 1024, true);
    }
}
