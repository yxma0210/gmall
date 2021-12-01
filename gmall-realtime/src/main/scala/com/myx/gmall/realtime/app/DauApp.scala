package com.myx.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.myx.gmall.realtime.bean.DauInfo
import com.myx.gmall.realtime.utils.{MyESUtil, MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

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
    // jsonObjDStream.print()

    //通过Redis   对采集到的启动日志进行去重操作  方案1  采集周期中的每条数据都要获取一次Redis的连接，连接过于频繁
    //redis类型:set  key：dau：2021-11-29  value: mid   expire   3600*24
    /*jsonObjDStream.filter{
      jsonObj => {
        // 获取登录日期
        val dt: String = jsonObj.getString("dt")
        // 获取设备id
        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        // 拼接Redis中保存的登录信息的key
        val dauKey = "dau:" + dt
        // 获取Jedis客户端
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        // 从redis中判断当前的设置是否登录过
        val isFirst: lang.Long = jedis.sadd(dauKey, mid)
        // 设置redis的实效时间
        if(jedis.ttl(dauKey) < 0) {
          jedis.expire(dauKey,3600*24)
        }
        // 关闭连接
        jedis.close()
        if (isFirst == 1L) {
          // 说明是第一次登录
          true
        } else {
          // 说明已经登录过了
          false
        }
      }
    }*/
    //通过Redis   对采集到的启动日志进行去重操作  方案2  以分区为单位对数据进行处理，每一个分区获取一次Redis的连接
    //redis类型:set  key：dau：2021-11-29  value: mid   expire   3600*24
    val filteredDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions {
      // 以分区为单位对数据进行处理
      jsonObjIter => {
        // 每一个分区获取一次redis的连接
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        //定义一个集合，用于存放当前分区中第一次登陆的日志
        val filteredList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
        // 对分区的数据进行遍历
        for (jsonObj <- jsonObjIter) {
          // 获取日期
          val dt: String = jsonObj.getString("dt")
          // 获取设备mid
          val mid: String = jsonObj.getJSONObject("common").getString("mid")
          // 拼接操作redis的key
          val daukdy = "dau:" + dt
          val isFirst: lang.Long = jedis.sadd(daukdy, mid)
          // 设置key的实效时间
          if (jedis.ttl(daukdy) < 0) {
            jedis.expire(daukdy, 3600 * 24)
          }
          if (isFirst == 1L) {
            // 说明是第一次登录
            filteredList.append(jsonObj)
          }
        }
        // 关闭连接
        jedis.close()
        filteredList.toIterator
      }
    }
    // filteredDStream.count().print()
    //将数据批量的保存到ES中
    filteredDStream.foreachRDD{
      rdd => {
        // 以分区为单位对数据进行处理
        rdd.foreachPartition{
          jsonObjItr => {
            val dauInfoList: List[(String, DauInfo)] = jsonObjItr.map {
              jsonObj => {
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                val dauInfo: DauInfo = DauInfo(
                  commonJsonObj.getString("mid"),
                  commonJsonObj.getString("uid"),
                  commonJsonObj.getString("ar"),
                  commonJsonObj.getString("ch"),
                  commonJsonObj.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00",
                  jsonObj.getLong("ts")
                )
                (dauInfo.mid, dauInfo)
              }
            }.toList

            // 将数据批量保存到ES中
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauInfoList,"gmall_dau_info_" + dt)
          }
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
