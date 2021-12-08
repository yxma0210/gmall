package com.myx.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.myx.gmall.realtime.utils.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Description: 从Kafka中读取数据，根据表名进行分流处理（maxwell）
 * @author: mayx
 * @date: 2021/12/7 22:58
 */
object BaseDBMaxwellApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("BaseDBMaxwellApp")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val topic = "gmall_dbm"
    val groupId = "base_db_maxwell_group"

    // 从redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      // 如果偏移量不为空，从当前偏移量开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      // 如果偏移量为空，从起始位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    // 获取当前采集周期中读取到的主题的分区以及偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    // 对读取的数据进行结构的转换   ConsumerRecord<K,V> ==>V(jsonStr)==>V(jsonObj)
    val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map {
      record => {
        val jsonStr: String = record.value()
        // 将json字符串转换为json对象
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    }
    // 分流,从json对象中获取table和data，发送到不同的kafka主题
    jsonObjDStream.foreachRDD{
      rdd => {
        rdd.foreach{
          jsonObj => {
            // 获取操作类型
            val opType: String = jsonObj.getString("type")
            // 获取操作的数据
            val dataJsonObj: JSONObject = jsonObj.getJSONObject("data")
            // 获取表名
            val table: String = jsonObj.getString("table")
            if (dataJsonObj!=null && !dataJsonObj.isEmpty && !"delete".equals(opType)) {
              // 拼接主题
              var sendTopic = "ods_" + table
              // 向Kafka发送数据
              MyKafkaSink.send(sendTopic,dataJsonObj.toString())
            }
          }
        }
        // 修改redis中Kafka的偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
