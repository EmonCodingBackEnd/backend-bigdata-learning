package com.coding.bigdata.redis;

import redis.clients.jedis.Jedis;

/** 单连接方式操作Redis */
public class RedisSingle {
    public static void main(String[] args) {
        // 获取jedis连接
        Jedis jedis = new Jedis("emon", 6379);
        jedis.auth("redis123");
        // 向redis中添加数据
        jedis.set("name", "lm");

        String value = jedis.get("name");
        System.out.println(value);
    }
}
