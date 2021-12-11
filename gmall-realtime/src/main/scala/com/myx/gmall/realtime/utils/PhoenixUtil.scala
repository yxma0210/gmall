package com.myx.gmall.realtime.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
 * @Description: 创建Phoenix查询工具
 * @author: mayx
 * @date: 2021/12/11 16:00
 */
object PhoenixUtil {
  def main(args: Array[String]): Unit = {
    val list: List[JSONObject] = queryList("select * from user_status")
    println(list)
  }
  /**
   *  期望结果：
   *   {"user_id":"zs","if_consumerd":"1"}
   *   {"user_id":"zs","if_consumerd":"1"}
   *   {"user_id":"zs","if_consumerd":"1"}
   * @param sql
   * @return
   */
  def queryList(sql: String):List[JSONObject] = {
    val reList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
    // 注册驱动
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    // 建立连接
    val conn: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop201,hadoop202,hadoop203:2181")
    // 创建数据库操作对象
    val statement: PreparedStatement = conn.prepareStatement(sql)
    // 执行sql语句
    val resultSet: ResultSet = statement.executeQuery()
    val metaData: ResultSetMetaData = resultSet.getMetaData
    // 处理结果集
    while (resultSet.next()) {
      val count: Int = metaData.getColumnCount
      val userStatusJsonObj = new JSONObject()
      for ( i<- 1 to count) {
        userStatusJsonObj.put(metaData.getColumnClassName(i),resultSet.getObject(i))
      }
      reList.append(userStatusJsonObj)
    }
    resultSet.close()
    statement.close()
    conn.close()
    reList.toList
  }
}
