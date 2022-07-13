package com.myx.gmall.realtime.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.myx.gmall.realtime.bean.{OrderInfo, ProvinceInfo, UserInfo, UserStatus}
import com.myx.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
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
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    // 对DS机构进行转换 DStream[ConsumerRecord[String, String]] --> values:jsonStr --> OrderInfo
    val orderInfoDStream: DStream[OrderInfo] = offsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        // 将json格式转换为OrderInfo对象
        val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
        // 2021-12-13 10:30:20
        val create_time: String = orderInfo.create_time
        val create_timeArr: Array[String] = create_time.split(" ")
        orderInfo.create_date = create_timeArr(0)
        orderInfo.create_hour = create_timeArr(1).split(":")(0)
        orderInfo
      }
    }
    // orderInfoDStream.print(1000)

    // 方案1：对DStream中的数据进行处理，判断下单的用户是否为首单
    // 对于每条订单都要执行一个sql，sql语句过多
    /*val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.map {
      orderInfo => {
        // 获取用户的id
        val userId: Long = orderInfo.user_id
        // 根据用户id到phoenix中查询是否下单过
        val sql = s"select user_id,if_consumed from user_status where user_id='${userId}'"
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        if (userStatusList != null && userStatusList.size > 0) {
          orderInfo.if_first_order = "0"
        } else {
          orderInfo.if_first_order = "1"
        }
        orderInfo
      }
    }*/

    // 方案2  以分区为单位，将整个分区的数据拼接一条SQL进行一次查询
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions {
      orderInfoIter => {
        // 当前一个分区中所有订单的集合
        val orderInfoList: List[OrderInfo] = orderInfoIter.toList
        // 获取当前分区中下单的用户
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        // 根据用户集合到Phoenix中查询出下过单的用户
        val sql: String =
          s"select user_id,if_consumed from user_status where user_id in('${userIdList.mkString("','")}')"
        val userStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        // 获取消费过的用户id
        val consumedUserIdList: List[String] = userStatusList.map(_.getString("USER_ID"))
        for (orderInfo <- orderInfoList) {
          if (consumedUserIdList.contains(orderInfo.user_id.toString)) {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoList.toIterator
      }
    }
    // orderInfoWithFirstFlagDStream.print(10000)

    /*
      ===================同一批次中状态修正  ====================
      应该将同一采集周期的同一用户的最早的订单标记为首单，其它都改为非首单
      	同一采集周期的同一用户-----按用户分组（groupByKey）
      	最早的订单-----排序，取最早（sortwith）
      	标记为首单-----具体业务代码
      */
    //对待处理的数据进行结构转换orderInfo====>(userId,orderInfo)
    val mapDStream: DStream[(Long, OrderInfo)] = orderInfoWithFirstFlagDStream.map(orderInfo => (orderInfo.user_id, orderInfo))
    // 根据用户id对用户进行分组
    val groupByKeyDStream: DStream[(Long, Iterable[OrderInfo])] = mapDStream.groupByKey()

    val sortOrderInfoWithFirstFlagDStream: DStream[OrderInfo] = groupByKeyDStream.flatMap {
      case (userId, orderInfoIter) => {
        val orderInfoList: List[OrderInfo] = orderInfoIter.toList
        // 怕暖在一个周期内，同一个用户是否存在多个订单
        if (orderInfoList.size > 1 && orderInfoList != null) {
          // 如果下了多个订单，按照订单时间升序排序
          val sortOrderInfoList: List[OrderInfo] = orderInfoList.sortWith {
            (orderInfo1, orderInfo2) => {
              orderInfo1.create_time < orderInfo2.create_time
            }
          }
          // 取出集合第一个元素
          if (sortOrderInfoList(0).if_first_order == "1") {
            // 时间最早的订单首单状态保留为1，其它的都设置为非首单
            for (i <- 1 until sortOrderInfoList.size) {
              sortOrderInfoList(i).if_first_order = "0"
            }
          }
          sortOrderInfoList
        } else {
          orderInfoList
        }
      }
    }
    // 省份表和维度表关联
    // 方案1：以分区为单位，对订单数据进行处理，和phoenix中的订单表关联
    /* val orderInfoWithProvinceDStream: DStream[OrderInfo] =
      sortOrderInfoWithFirstFlagDStream.mapPartitions {
      orderInfoIter => {
        // 转换为List
        val orderInfoList: List[OrderInfo] = orderInfoIter.toList
        // 获取当前分区中订单对应的省份id
        val provinceIdList: List[Long] = orderInfoList.map(_.province_id)
        // 根据省份id到phoenix中查询对应的省份
        var sql: String = s"select id,name,area_code,iso_code from gmall_province_info where id in('${provinceIdList.mkString("','")}')"
        val privinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val provinceInfoMap: Map[String, ProvinceInfo] = privinceInfoList.map {
          provinceJsonObj => {
            // 将json对象转换为省份样例类对象
            val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])
            (provinceInfo.id, provinceInfo)
          }
        }.toMap

        // 对订单数据进行遍历，用遍历的省份id，provinceInfoMap获取省份对象
        for (orderInfo <- orderInfoList) {
          val proInfo: ProvinceInfo = provinceInfoMap.getOrElse(orderInfo.province_id.toString, null)
          if (proInfo != null) {
            orderInfo.province_name = proInfo.name
            orderInfo.province_area_code = proInfo.area_code
            orderInfo.province_iso_code = proInfo.iso_code
          }
        }
        orderInfoList.toIterator
      }
    } */

    // 关联省份方案2 使用广播变量，在Driver端进行一次查询，分区越多效果月明显，前提：省份数据量少
    val orderInfoWithProvinceDStream: DStream[OrderInfo] = sortOrderInfoWithFirstFlagDStream.transform {
      rdd => {
        // 从Phoenix中查询所有的省份数据
        val sql: String = "select id,name, area_code, iso_code from gmall_province_info"
        val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val provinceInfoMap: Map[String, ProvinceInfo] = provinceInfoList.map {
          provinceJsonObj => {
            // 将json对象转换为省份样例类对象
            val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])
            (provinceInfo.id, provinceInfo)
          }
        }.toMap
        // 定义省份的广播变量
        val dbMap: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceInfoMap)

        rdd.map {
          orderInfo => {
            val proInfo: ProvinceInfo = dbMap.value.getOrElse(orderInfo.province_id.toString, null)
            if (proInfo != null) {
              orderInfo.province_name = proInfo.name
              orderInfo.province_area_code = proInfo.area_code
              orderInfo.province_iso_code = proInfo.iso_code
            }
            orderInfo
          }
        }
      }
    }

    // orderInfoWithProvinceDStream.print(1000)

    // 用户维度表进行关联
    // 以分区为单位对数据进行处理，每个分区拼接一个sql到phoenix上查询用户数据
    val orderInfoWithUserInfoDStream: DStream[OrderInfo] = orderInfoWithProvinceDStream.mapPartitions {
      orderInfoItr => {
        // 转化为list集合
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        // 获取所有用户id
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        // 根据id拼接sql语句，到phoenix查询用户
        val sql: String = s"select id,user_level,birthday,gender,age_group," +
          s"gender_name from gmall_user_info where id in (${userIdList.mkString("','")})"
        // 当前分区中所有的下单用户
        val userList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userMap: Map[String, UserInfo] = userList.map {
          userJsonObj => {
            val userInfo: UserInfo = JSON.toJavaObject(userJsonObj, classOf[UserInfo])
            (userInfo.id, userInfo)
          }
        }.toMap
        for (orderInfo <- orderInfoList) {
          val userInfoObj: UserInfo = userMap.getOrElse(orderInfo.user_id.toString, null)
          if (userInfoObj != null) {
            orderInfo.user_age_group = userInfoObj.age_group
            orderInfo.user_gender = userInfoObj.gender_name
          }
        }
        orderInfoList.toIterator
      }
    }

    orderInfoWithUserInfoDStream.print(1000)

    // 保存用户状态
    import org.apache.phoenix.spark._
    orderInfoWithProvinceDStream.foreachRDD{
      rdd => {
        // 从所有的订单中，将首订单过滤出来
        val firstOrderRDD: RDD[OrderInfo] = rdd.filter(_.if_first_order == "1")
        // 获取当前用户并更新到Hbase，注意saveToPhoenix在更新的时候，要求RDD的属性和插入hbase表中的列数必须保持一致，
        // 所以转换一下
        val firstOrderUserRDD: RDD[UserStatus] = firstOrderRDD.map {
          orderInfo => UserStatus(orderInfo.user_id.toString, "1")
        }
        firstOrderUserRDD.saveToPhoenix(
          "USER_STATUS",
          Seq("USER_ID","IF_CONSUMED"),
          new Configuration,
          Some("hadoop201,hadoop202,hadoop203:2181")
        )
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
