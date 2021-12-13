package com.myx.gmall.realtime.dwd

import com.alibaba.fastjson.JSON
import com.myx.gmall.realtime.bean.OrderInfo
import com.myx.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Description: 从Kafka中读取订单数据，并对其进行处理
 * @author: mayx
 * @date: 2021/12/13 21:42
 */
object OrderInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderInfoApp")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topic = "ods_order_info"
    val groupId = "order_info_group"
    // 从redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    // 从Kafka主题中读取数据
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      // 如果Redis中存在当前消费者组对该主题的偏移量信息，那么从当前位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      // 如果Redis中不存在当前消费者组对该主题的偏移量信息，那么从开始位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    // 获取当前批次处理的偏移量
    var offsetRanges = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    // 对DS机构进行转换 DStream[ConsumerRecord[String, String]] --> values:jsonStr --> OrderInfo
    val orderInfoDStream: DStream[Unit] = offsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        // 将json格式转换为OrderInfo对象
        val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
        // 2021-12-13 10:30:20
        val create_time: String = orderInfo.create_time
        val create_timeArr: Array[String] = create_time.split(" ")
        orderInfo.create_date = create_timeArr(0)
        orderInfo.create_hour = create_timeArr(1).split(":")(0)
      }
    }
    orderInfoDStream.print(1000)
  }
}
