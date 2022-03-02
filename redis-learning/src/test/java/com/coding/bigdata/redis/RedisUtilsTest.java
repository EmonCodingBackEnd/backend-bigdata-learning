package com.coding.bigdata.redis;

import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

public class RedisUtilsTest {

    @Test
    void test() {
        Jedis jedis = RedisUtils.getJedis();
        String value = jedis.get("name");
        System.out.println(value);
        RedisUtils.returnResource(jedis);
    }
}
