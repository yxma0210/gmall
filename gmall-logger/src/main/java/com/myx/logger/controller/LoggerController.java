package com.myx.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;




/**
 * @author mayx
 * @PackageName:com.myx.gmall.controller
 * @ClassName: LoggerController
 * @Description:  该Controller用于接收模拟生成的日志
 * @date 2021/11/20 20:12
 */
//@Controller   将对象的创建交给Spring容器  方法返回String，默认会当做跳转页面处理
//@RestController =  @Controller + @ResponseBoby    方法返回Object，底层会转换为json格式字符串进行相应
@RestController
@Slf4j
public class LoggerController {
    @Autowired // 将KafkaTemplate注入到Controller中
    KafkaTemplate kafkaTemplate;
    //通过requestMapping匹配请求并交给方法处理

    //http://localhost:8080/applog
    //提供一个方法，处理模拟器生成的数据
    //@RequestMapping("/applog")  把applog请求，交给方法进行处理
    //@RequestBody   表示从请求体中获取数据
    @RequestMapping("/applog")
    public String applog(@RequestBody String mockLog) {
        // 落盘

        log.info(mockLog);
        //根据日志的类型，发送到kafka的不同主题中去
        //将接收到的字符串数据转换为json对象
        JSONObject jsonObject = JSON.parseObject(mockLog);
        JSONObject startJson = jsonObject.getJSONObject("start");
        if (startJson != null) {
            // 启动日志
            kafkaTemplate.send("gmall_start", mockLog);
        } else {
            // 事件日志
            kafkaTemplate.send("gmall_event", mockLog);
        }
        return "success";
    }
}
