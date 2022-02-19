package com.coding.bigdata.common;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;

@Slf4j
public final class EnvUtils {

    public static boolean isWin = true;

    static {
        String os = System.getProperty("os.name");
        if (os.toLowerCase().startsWith("win")) {
            isWin = true;
        } else {
            isWin = false;
        }
    }

    public static final String HDFS_PATH = "hdfs://emon:8020";

    public static String toHDFSPath(@NonNull String path) {
        if (path.startsWith(HDFS_PATH)) {
            return path;
        }
        if (path.startsWith(".")) {
            path = path.substring(1);
        }
        if (path.startsWith("/")) {
            return HDFS_PATH + path;
        }
        return HDFS_PATH + "/" + path;
    }

    /**
     * 根据系统环境构造Hadoop的conf对象
     *
     * @param isWinLocal - 是否在Win本地环境处理
     * @return - Configuration
     */
    public static Configuration buildConfByEnv(boolean isWinLocal) {
        Configuration configuration;
        configuration = new Configuration();
        if (EnvUtils.isWin) {
            System.setProperty(
                    "hadoop.home.dir", "C:\\Job\\JobSoftware\\hadoop-common-2.2.0-bin-master");
            System.setProperty("HADOOP_USER_NAME", "emon");
            configuration.set("dfs.replication", "1"); // 默认3
            if (!isWinLocal) {
                configuration.set("fs.defaultFS", HDFS_PATH);
            }
        } else {
            configuration.set("fs.defaultFS", HDFS_PATH);
        }
        return configuration;
    }

    /**
     * 根据系统环境构造Hadoop的FileSystem对象
     *
     * @param isWinLocal - 是否在Win本地环境处理
     * @return - FileSystem
     */
    public static FileSystem buildFileSystemByEnv(boolean isWinLocal) {
        FileSystem fileSystem;
        try {
            Configuration configuration = buildConfByEnv(isWinLocal);
            // fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "emon");
            fileSystem = FileSystem.get(configuration);
        } catch (IOException /*| URISyntaxException | InterruptedException*/ e) {
            log.error("初始化fileSystem异常", e);
            throw new RuntimeException("初始化fileSystem异常", e);
        }
        return fileSystem;
    }

    private static void ensureFileExists(String filePath, boolean isWinLocal) throws IOException {
        if (isWinLocal) {
            File inputFile = new File(filePath);
            if (!inputFile.exists()) {
                log.error("local目录/文件不存在 filePath={}", filePath);
                throw new RuntimeException("目录/文件不存在");
            }
        } else {
            FileSystem fileSystem = EnvUtils.buildFileSystemByEnv(false);
            Path inputHdfsPath = new Path(filePath);
            if (!fileSystem.exists(inputHdfsPath)) {
                log.error("hdfs目录/文件不存在 filePath={}", filePath);
            }
        }
    }

    private static void ensureFileNotExists(String filePath, boolean isWinLocal)
            throws IOException {
        if (isWin && isWinLocal) {
            File outputFile = new File(filePath);
            if (outputFile.isDirectory() && outputFile.exists()) {
                boolean deleted = FileUtils.deleteQuietly(outputFile);
                if (deleted) {
                    log.warn("local目录/文件已存在，清理成功！filePath={}", outputFile.getAbsolutePath());
                } else {
                    log.error("local目录/文件已存在，清理失败！filePath={}", outputFile.getAbsolutePath());
                }
            }
        } else {
            FileSystem fileSystem = EnvUtils.buildFileSystemByEnv(false);
            Path outputHdfsPath = new Path(filePath);
            if (fileSystem.exists(outputHdfsPath)) {
                boolean deleted = fileSystem.delete(outputHdfsPath, true);
                if (deleted) {
                    log.warn("hdfs目录/文件已存在，清理成功！outputPath={}", filePath);
                } else {
                    log.error("hdfs目录/文件已存在，清理失败！outputPath={}", filePath);
                }
            }
        }
    }

    /**
     * 校验输入目录/文件是否合法：存在该文件，或者该目录包含文件
     *
     * @param inputPath - 输入目录/文件
     * @param isWinLocal - 是否在Win本地环境处理
     */
    public static void checkInputPath(String inputPath, boolean isWinLocal) {
        try {
            if (EnvUtils.isWin) {
                EnvUtils.buildConfByEnv(isWinLocal);
            }
            ensureFileExists(inputPath, isWinLocal);
        } catch (IOException e) {
            log.error("校验输入目录/文件异常", e);
            throw new RuntimeException("文件异常", e);
        }
    }

    /**
     * 校验输出目录是否合法：尚未存在该目录/文件，如果存在则先清理！
     *
     * @param outputPath - 输出目录/文件
     * @param isWinLocal - 是否在Win本地环境处理
     */
    public static void checkOutputPath(String outputPath, boolean isWinLocal) {
        try {
            if (EnvUtils.isWin) {
                EnvUtils.buildConfByEnv(isWinLocal);
            }
            ensureFileNotExists(outputPath, isWinLocal);
        } catch (IOException e) {
            log.error("为MR任务准备坏境异常", e);
            throw new RuntimeException("为MR任务准备坏境异常", e);
        }
    }

    public static SparkConf buildSparkConfByEnv(String appName) {
        SparkConf sparkConf = new SparkConf();
        if (EnvUtils.isWin) {
            System.setProperty(
                    "hadoop.home.dir", "C:\\Job\\JobSoftware\\hadoop-common-2.2.0-bin-master");
            System.setProperty("HADOOP_USER_NAME", "emon");
            sparkConf
                    .setAppName(appName) // 设置任务名称
                    /*
                     * 解决bug
                     * A master URL must be set in your configuration
                     */
                    .setMaster("local") // local[2]表示本地执行，使用2个工作线程的
                    // StreamingContext；local表示1个工作线程；local[*]自动获取
                    /*
                     * 解决bug
                     * java.lang.IllegalArgumentException: System memory 259522560 must be at least 471859200. Please increase heap size using the --driver-memory option or spark.driver.memory in Spark configuration.
                     */
                    .set("spark.testing.memory", 512 * 1024 * 1024 + "");
        }
        return sparkConf;
    }

    public static SparkSession buildSparkSessionByEnv(String appName) {
        SparkSession.Builder builder = SparkSession.builder();
        if (EnvUtils.isWin) {
            System.setProperty(
                    "hadoop.home.dir", "C:\\Job\\JobSoftware\\hadoop-common-2.2.0-bin-master");

            builder.appName(appName)
                    .master("local") // local[2]表示本地执行，使用2个工作线程的
                    // StreamingContext；local表示1个工作线程；local[*]自动获取
                    .config("spark.testing.memory", 512 * 1024 * 1024 + "");
        }

        SparkSession sparkSession = builder.getOrCreate();

        if (EnvUtils.isWin) {
            // 默认分区200个，对于小作业来说，大部分时间花费到了任务调度上；设置初始分区数的1.5-2倍
            String key = "spark.sql.shuffle.partitions";
            sparkSession.conf().set(key, 2);
            System.out.println(key + " = " + sparkSession.conf().get(key));
        }

        return sparkSession;
    }
}
