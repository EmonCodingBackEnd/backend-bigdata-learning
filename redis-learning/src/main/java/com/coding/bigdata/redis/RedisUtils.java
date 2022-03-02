package com.coding.bigdata.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

/** 基于Redis连接池提取Redis工具类 */
public class RedisUtils {
    // 私有化构造函数，禁止new
    private RedisUtils() {}

    private static JedisPool JEDIS_POOL;

    // 获取连接
    public static synchronized Jedis getJedis() {
        if (JEDIS_POOL == null) {
            // 创建连接池配置对象
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            // 连接池中最大空闲连接数
            poolConfig.setMaxIdle(10);
            // 连接池中创建的最大连接数
            poolConfig.setMaxTotal(100);
            // 创建连接的超时时间
            poolConfig.setMaxWait(Duration.ofMillis(2000));
            // 表示从连接池中获取连接的时候会先测试一下连接是否可用，这样可以保证取出的连接都是可用的。
            poolConfig.setTestOnBorrow(true);
            // 获取jedis连接池
            JEDIS_POOL = new JedisPool(poolConfig, "emon", 6379, 10000, "redis123");
        }
        // 从jedis连接池中获取一个连接
        return JEDIS_POOL.getResource();
    }

    // 向连接池返回连接
    public static void returnResource(Jedis jedis) {
        jedis.close();
    }
}
