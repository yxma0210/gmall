package com.myx.gmall.realtime.utils

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
 * @Description:  向Kafka主题中发送数据
 * @author: mayx
 * @date: 2021/12/7 22:01
 */
object MyKafkaSink {
  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")
  var kafkaProducer: KafkaProducer[String, String] = null

  // kafka生产者配置
  def createKafkaProducer: KafkaProducer[String, String] = {
    val properties = new Properties
    //用于初始化链接到集群的地址
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,broker_list)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,(true: java.lang.Boolean))
    var producer: KafkaProducer[String, String] = null
    try
      producer = new KafkaProducer[String, String](properties)
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    producer
  }

  def send(topic: String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, msg))
  }

  def send(topic: String,key:String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic,key, msg))
  }
}
