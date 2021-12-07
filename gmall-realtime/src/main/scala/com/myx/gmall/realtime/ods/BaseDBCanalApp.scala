package com.myx.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.myx.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Description: 从Kafka中读取数据，根据表名进行分流处理（canal）
 * @author: mayx
 * @date: 2021/12/7 16:37
 */
object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("BaseDBCanalApp")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topic = "gmall_dblog"
    val groupId = "base_db_canal_group"

    // 从redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0 ) {
      // 从指定的偏移量位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap,
        groupId)
    } else {
      // 从最新的位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    // 获取当前批次读取的kafka主题中的偏移量信息
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    // 对接收的数据进行结构的转换 ConsumerRecord[String,String(jsonStr)]====>jsonObj
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        // 获取json格式的字符串
        val jsonStr: String = record.value()
        // 将json格式字符串转换为json对象
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    }

    // 分流：根据不同的表名，将数据发送到不同的kafka主题中
    jsonObjDStream.foreachRDD{
      rdd => {
        rdd.foreach{
          jsonObj => {
            // 获取操作类型
            val opType: String = jsonObj.getString("type")
            if ("INSERT".equals(opType)) {
              // 获取表名
              val table: String = jsonObj.getString("table")
              // 获取操作数据
              val dataArr: JSONArray = jsonObj.getJSONArray("data")
              // 拼接主题
              var sendTopic = "ods_" + table
              // 对数据进行遍历
              import scala.collection.JavaConverters._
              for (dataJson <- dataArr.asScala) {
                // 根据表名将数据发送到不同的主题中
                MyKafkaSend(sendTopic,dataJson.toString)
              }
            }
          }
        }
        // 提交偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
