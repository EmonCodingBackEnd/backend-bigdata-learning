package com.coding.bigdata.flink.stream.sink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/*
 * 需求：接收Socket传输过来的数据，把数据保存到Redis的List队列中
 *
 * 前提：在启动该程序之前，先在指定主机emon启动命令： nc -lk 9000
 * 等启动程序后，在emon主机终端输入： hello you hello me
 */
object StreamRedisSinkScala {

  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {
    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 连接socket获取输入数据
    val text = env.socketTextStream("emon", 9000)

    // 组装数据，这里组装的是tuple2类型
    // 第一个元素是指list队列的key名称
    // 第二个元素是指需要向list队列中添加的元素
    val listData: DataStream[(String, String)] = text.map(word => ("l_words_scala", word))

    // 指定redisSink
    val conf = new FlinkJedisPoolConfig.Builder().setHost("emon").setPort(6379).setPassword("redis123").build()
    val redisSink: RedisSink[(String, String)] = new RedisSink[Tuple2[String, String]](conf, new MyRedisMapper)
    listData.addSink(redisSink)

    env.execute(this.getClass.getSimpleName)
  }

  class MyRedisMapper extends RedisMapper[Tuple2[String, String]] {

    // 指定具体的操作命令
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.LPUSH)
    }

    // 获取key
    override def getKeyFromData(data: (String, String)): String = {
      data._1
    }


    override def getValueFromData(data: (String, String)): String = {
      data._2
    }
  }
}
