package com.coding.bigdata.hive;

import lombok.extern.slf4j.Slf4j;

import java.sql.*;

/*
 * JDBC代码操作Hive
 * 需要先启动 hiveserver2 服务
 * 预先创建表和数据：
 * hive> create table t1(id int, name string);
 * insert into t1(id,name) values(1, 'zs');
 */
@Slf4j
public class HiveJdbcDemo {
    public static void main(String[] args) throws SQLException {
        // 指定 hiveserver2 的url链接
        String jdbcUrl = "jdbc:hive2://emon:10000";
        // 获取链接，这里的user使用emon，就是linux中的用户名，password随便指定即可。
        Connection connection = DriverManager.getConnection(jdbcUrl, "emon", "any");

        // 获取Statement
        Statement stmt = connection.createStatement();

        // 指定查询的sql
        String sql = "select * from t1";
        // 执行sql
        ResultSet res = stmt.executeQuery(sql);
        // 循环读取结果
        while (res.next()) {
            System.out.println(res.getInt("id") + "\t" + res.getString("name"));
        }
        log.info("完成！");
    }
}
