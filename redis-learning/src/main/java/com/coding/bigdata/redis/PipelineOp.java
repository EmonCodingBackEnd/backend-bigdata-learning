package com.coding.bigdata.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/** Pipeline（管道的使用） */
public class PipelineOp {
    public static void main(String[] args) {
        // 1：不适用管道
        Jedis jedis = RedisUtils.getJedis();

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            jedis.set("a" + i, "a" + i);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("不使用管道，耗时 " + (endTime - startTime) + " ms");

        // 2：使用管道
        Pipeline pipelined = jedis.pipelined();
        startTime = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            pipelined.set("b" + i, "b" + i);
        }
        pipelined.sync();
        endTime = System.currentTimeMillis();
        System.out.println("使用管道，耗时 " + (endTime - startTime) + " ms");

        RedisUtils.returnResource(jedis);

        /*
        不使用管道，耗时 42556 ms
        使用管道，耗时 173 ms
         */
    }
}
