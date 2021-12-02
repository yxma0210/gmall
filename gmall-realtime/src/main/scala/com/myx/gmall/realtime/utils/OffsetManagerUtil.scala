package com.myx.gmall.realtime.utils

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
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

  /**
   * 将偏移量信息保存到Redis中
   * @param topic
   * @param groupId
   * @param offsetRanges
   */
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    // 拼接Redis中操作偏移量的字符串
    var offsetKey = "offset:" + topic + ":" + groupId
    // 定义java的map集合，用于存放每个分区对应的偏移量
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String, String]()
    // 对offsetRanges进行遍历，将数据封装到offsetMap
    for (offsetRange <- offsetRanges) {
      // 获取分区
      val partitionId: Int = offsetRange.partition
      // 获取起始偏移量
      val fromOffset: Long = offsetRange.fromOffset
      // 获取结束偏移量
      val untilOffset: Long = offsetRange.untilOffset
      offsetMap.put(partitionId.toString,untilOffset.toString)
      println("保存分区" + partitionId + ":" + fromOffset + "----->" + untilOffset)
    }
    // 获取客户端连接
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    jedis.hmset(offsetKey,offsetMap)
    // 关闭连接
    jedis.close()
  }
}
