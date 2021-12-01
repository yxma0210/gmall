package com.myx.gmall.realtime.utils

import java.util

import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis

/**
 * @Description: 偏移量管理类，用于读取和保存偏移量
 * @author: mayx
 * @date: 2021/12/1 22:51
 */
object OffsetManagerUtil {
  // 从Redis中获取偏移量
  // type:hash   key: offset:topic:groupId   field:partition   value: 偏移量
  // offsets:Map[TopicPartition,Long] 根据kafka工具类指定返回结果的类型
  def getOffset(topic:String,groupId:String): Map[TopicPartition,Long]= {
    // 获取客户端连接
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    // 拼接Redis的key   offset:topic:groupId
    val offsetKey = "offset:" + topic + ":" + groupId
    // 获取当前消费者组消费的主题   对应的分区以及偏移量
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    // 关闭客户端连接
    jedis.close()

    import scala.collection.JavaConverters._
    val oMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, offset) => {
        println("读取分区偏移量：" + partition + ":" + offset)
        // Map[TopicPartition,Long]
        (new TopicPartition(topic, partition.toInt), offset.toLong)
      }
    }.toMap
    oMap
  }
}
