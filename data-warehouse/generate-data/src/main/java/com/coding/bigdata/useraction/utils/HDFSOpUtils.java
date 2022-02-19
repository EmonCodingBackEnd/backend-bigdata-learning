package com.coding.bigdata.useraction.utils;

import com.coding.bigdata.common.EnvUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class HDFSOpUtils {

    public static void put(String inContent, String targetPath, String fileName)
            throws IOException {
        boolean isWinLocal = false;
        FileSystem fileSystem = EnvUtils.buildFileSystemByEnv(isWinLocal);
        Path f = new Path(targetPath);
        // 尝试删除，再创建目标目录
        fileSystem.delete(f, true);
        fileSystem.mkdirs(f);

        // 获取输入流
        ByteArrayInputStream fis =
                new ByteArrayInputStream(inContent.getBytes(StandardCharsets.UTF_8));
        // 获取HDFS文件系统的输出流
        FSDataOutputStream fsdos = fileSystem.create(new Path(targetPath + "/" + fileName));

        // 上传文件：通过工具类把输入流拷贝到输出流里面，实现本地文件上传到HDFS
        IOUtils.copyBytes(fis, fsdos, 1024, true);
    }
}
