package com.myx.gmall.realtime.dwd

import com.alibaba.fastjson.JSON
import com.myx.gmall.realtime.bean.ProvinceInfo
import com.myx.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Description: 从Kafka中读取省份数据，保存到Phoenix
 * @author: mayx
 * @date: 2021/12/15 13:53
 */
object ProvinceInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("ProvinceInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    var topic: String = "ods_base_province"
    var groupId: String = "province_info_group"
    // 从kafka中读取数据
    // 从Redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    // 根据偏移量获取数据
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      // 如果Redis中存在当前消费者组对该主题的消费者的偏移量信息，那么从当前位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      // 如果Redis中不存在当前消费者组对该主题的消费者的偏移量信息，那么从开始位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    // 得到本批次中处理数据的分区对应的偏移量起始及结束位置
    // 注意：这里我们从Kafka中读取数据之后，直接就获取了偏移量的位置，因为KafkaRDD可以转换为HasOffsetRanges，会自动记录位置
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] =
      recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    // 保存数据到Hbase中
    offsetDStream.foreachRDD{
      rdd => {
        val provinceInfoRDD: RDD[ProvinceInfo] = rdd.map {
          record => {
            // 获取省份的json格式字符串
            val jsonStr: String = record.value()
            // 将json格式字符串封装为ProvinceInfo对象
            val provinceInfo: ProvinceInfo = JSON.parseObject(jsonStr, classOf[ProvinceInfo])
            provinceInfo
          }
        }
        import org.apache.phoenix.spark._
        provinceInfoRDD.saveToPhoenix(
          "GMALL_PROVINCE_INFO",
          Seq("ID","NAME","AREA_CODE","ISO_CODE"),
          new Configuration,
          Some("hadoop201,hadoop202,hadoop203:2181")
        )
        // 保存偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
