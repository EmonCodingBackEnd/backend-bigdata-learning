package com.coding.bigdata.useraction.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayListHandler;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

// 拒绝继承
@Slf4j
public final class MyDBUtils {

    public static String className = "";
    public static String url = "";
    public static String user = "";
    public static String password = "";

    static {
        try {
            Properties prop = new Properties();
            prop.load(MyDBUtils.class.getClassLoader().getResourceAsStream("db.properties"));
            className = prop.getProperty("className");
            url = prop.getProperty("url");
            user = prop.getProperty("user");
            password = prop.getProperty("password");
        } catch (IOException e) {
            log.error("db.properties文件读取异常..." + e.getMessage());
        }
    }

    private static QueryRunner queryRunner = new QueryRunner();

    // 拒绝new一个实例
    private MyDBUtils() {}

    // 调用该类时即注册驱动
    static {
        try {
            Class.forName(className);
        } catch (ClassNotFoundException e) {
            log.error("DriverClass注册失败..." + e.getMessage());
            throw new RuntimeException();
        }
    }

    public static void main(String[] args) {
        List<Object[]> objects = executeQuerySql("select * from mysql.user");
        System.out.println(objects.size());
    }

    public static List<Object[]> executeQuerySql(String sql) {
        List<Object[]> result = new ArrayList<>();

        try {
            List<Object[]> requestList =
                    queryRunner.query(
                            getConnection(),
                            sql,
                            new ArrayListHandler(
                                    new BasicRowProcessor() {
                                        @Override
                                        public <T> List<T> toBeanList(
                                                ResultSet rs, Class<? extends T> type)
                                                throws SQLException {
                                            return super.toBeanList(rs, type);
                                        }
                                    }));
            result.addAll(requestList);
        } catch (SQLException e) {
            log.error("sql查询失败：" + sql + ",", e);
        }
        return result;
    }

    public static void update(String sql) {
        Connection connection = null;

        try {
            connection = getConnection();
            queryRunner.update(connection, sql);
        } catch (SQLException e) {
            log.error("sql更新失败：" + sql + ",", e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.error("conn链接关闭错误！", e);
                }
            }
        }
    }

    public static void batchUpdate(List<String> sqlList) {
        Connection connection = null;

        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            for (String sql : sqlList) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            log.error("sql批量执行失败：" + sqlList.get(0) + ",", e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.error("conn链接关闭错误！", e);
                }
            }
        }
    }

    // 获取链接
    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
}
