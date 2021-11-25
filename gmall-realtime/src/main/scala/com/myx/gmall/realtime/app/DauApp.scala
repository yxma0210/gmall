package com.myx.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.myx.gmall.realtime.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Description: 日活业务
 * @author: mayx
 * @date: 2021/11/25 23:08
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("DauApp")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    // 消费Kakfa数据
    var topic: String = "gmall_start"
    var groupId: String = "gmall_dau"
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

    val jsonObjDStream: DStream[JSONObject] = recordDStream.map {
      record => {
        // 获取启动日志
        val jsonStr: String = record.value()
        // 将启动日志转化为Json对象
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        // 获取毫秒数
        val ts: lang.Long = jsonObj.getLong("ts")
        // 获取字符串  日期  小时
        val dateHour: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        //对字符串日期和小时进行分割，分割后放到json对象中，方便后续处理
        val dateStrArr: Array[String] = dateHour.split(" ")
        var dt = dateStrArr(0)
        var hr = dateStrArr(1)
        jsonObj.put("dt", dt)
        jsonObj.put("hr", hr)
        jsonObj
      }
    }
    // 测试输出
    jsonObjDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
