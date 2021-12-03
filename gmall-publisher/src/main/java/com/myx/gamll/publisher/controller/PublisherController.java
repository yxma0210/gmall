package com.myx.gamll.publisher.controller;

import com.myx.gamll.publisher.service.ESService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description: 发布数据接口
 * @author: mayx
 * @date: 2021/12/3 16:15
 */
@RestController
public class PublisherController {
    @Autowired
    ESService esService;
      /*
        访问路径：http://publisher:8070/realtime-total?date=2019-02-01
        响应数据：[{"id":"dau","name":"新增日活","value":1200},
                    {"id":"new_mid","name":"新增设备","value":233} ]
     */

    @RequestMapping("/realtime-total")
    public Object realtimeTotal(@RequestParam("date") String dt) {
        ArrayList<Map<String, Object>> rsList = new ArrayList<>();
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        Long dauTotal = esService.getDauTotal(dt);
        if (dauTotal == null) {
            dauMap.put("value", 0L);
        } else {
            dauMap.put("value", dauTotal);
        }
        rsList.add(dauMap);

        Map<String,Object> midMap = new HashMap<>();
        midMap.put("id","new_mid");
        midMap.put("name","新增设备");
        midMap.put("value",666);
        rsList.add(midMap);

        return rsList;
    }
}
