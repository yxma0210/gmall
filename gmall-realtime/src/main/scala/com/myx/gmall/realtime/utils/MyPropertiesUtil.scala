package com.myx.gmall.realtime.utils

import java.io.InputStreamReader
import java.util.Properties

import com.nimbusds.jose.util.StandardCharset

/**
 * @Description: 读取配置文件的工具类
 * @author: mayx
 * @date: 2021/11/25 12:02
 */
object MyPropertiesUtil {
  def main(args: Array[String]): Unit = {
    val properties: Properties = MyPropertiesUtil.load("config.properties")
    println(properties)
  }

  def load(propertiesName: String): Properties = {
    val properties: Properties = new Properties()
    // 加载制定的配置文件
    properties.load(new InputStreamReader(Thread.currentThread()
      .getContextClassLoader.getResourceAsStream(propertiesName),
      StandardCharset.UTF_8))
    properties
  }
}
