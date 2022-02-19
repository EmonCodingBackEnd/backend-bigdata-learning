package com.coding.bigdata.hadoop.hdfs;

import com.coding.bigdata.common.EnvUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class HdfsTests {

    FileSystem fileSystem = null;

    @BeforeEach
    void setUp() {
        fileSystem = EnvUtils.buildFileSystemByEnv(false);
    }

    @AfterEach
    void tearDown() {
        fileSystem = null;
    }

    @Test
    void testUpload() throws Exception {
        FileInputStream fis = new FileInputStream("../custom/data/hdfs/user.txt");
        FSDataOutputStream fsdos = fileSystem.create(new Path("/custom/data/hdfs/user.txt"));
        // 上传文件：通过工具类把输入流拷贝到输出流里面，实现本地文件上传到HDFS
        IOUtils.copyBytes(fis, fsdos, 1024, true);
    }

    @Test
    void testDownload() throws IOException {
        String path = "/custom/data/hdfs/user.txt";
        if (fileSystem.exists(new Path(path))) {
            FSDataInputStream fsdis = fileSystem.open(new Path(path));
            FileOutputStream fos = new FileOutputStream("../custom/data/hdfs/user.down.txt");
            IOUtils.copyBytes(fsdis, fos, 1024, true);
        } else {
            System.out.println("文件不存在，无法下载！");
        }
    }

    @Test
    void testDelete() throws IOException {
        // 删除文件，目录也可以删除；如果递归删除目录，第二个参数 true；否则 false；如果是删除文件，不影响
        boolean deleted = fileSystem.delete(new Path("/custom/data/hdfs/user.txt"), true);
        if (deleted) {
            System.out.println("删除成功");
        } else {
            System.out.println("删除失败");
        }
    }
}
