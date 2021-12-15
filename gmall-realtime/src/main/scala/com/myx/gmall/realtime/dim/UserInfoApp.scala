package com.myx.gmall.realtime.dim

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.myx.gmall.realtime.bean.UserInfo
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
 * @Description: 从Kafka中读取用户数据，保存到Phoenix
 * @author: mayx
 * @date: 2021/12/15 22:34
 */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("UserInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    var topic = "ods_user_info"
    var groupId = "user_info_group"

    // 从redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    // 根据偏移量获取数据
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      // 如果redis中存在当前消费者组对该主题消费者偏移量，则从当前的偏移量消费数据
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      // 如果redis中不存在当前消费者组对该主题消费者偏移量，则从开始位置开始消费数据
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    // 获取本批次中处理数据的分区对应的偏移量起始及结束位置
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    // 保存数据到Hbase中
    offsetDStream.foreachRDD{
      rdd => {
        val userInfoRDD: RDD[UserInfo] = rdd.map {
          record => {
            // 获取用户的json格式字符串
            val jsonStr: String = record.value()
            // 将json格式字符串转化为json对象
            val userInfo: UserInfo = JSON.parseObject(jsonStr, classOf[UserInfo])
            // 把生日转成年龄
            val birthday: String = userInfo.birthday
            val format = new SimpleDateFormat("yyyy-MM-dd")
            val date: Date = format.parse(birthday)
            val currentTime: Long = System.currentTimeMillis()
            val betweenTs = currentTime - date.getTime
            val age = betweenTs / 60L / 60L / 24L / 365L / 1000L
            if (age < 20) {
              userInfo.age_group = "20岁及以下"
            } else if (age > 30) {
              userInfo.age_group = "30岁及以上"
            } else {
              userInfo.age_group = "在20岁和30岁之间"
            }

            // 处理性别
            val gender: String = userInfo.gender
            if (gender == "M") {
              userInfo.gender_name = "男"
            } else {
              userInfo.gender_name = "女"
            }
            userInfo
          }
        }
        // 保存到Phoenix中
        import org.apache.phoenix.spark._
        userInfoRDD.saveToPhoenix(
          "GMALL_USER_INFO",
          Seq("ID","USER_LEVEL","BIRTHDAY","GENDER","AGE_GROUP","GENDER_NAME"),
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
