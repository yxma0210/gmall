package com.myx.gamll.publisher.controller;

import com.myx.gamll.publisher.service.ESService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
        访问路径：http://publisher:8070/realtime-total?date=2021-12-03
        响应数据：[{"id":"dau","name":"新增日活","value":1200},
                    {"id":"new_mid","name":"新增设备","value":233} ]
     */

    /**
     * 新增日活
     * @param dt
     * @return
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

    /*
        访问路径：http://publisher:8070/realtimeHour?id=dau&date=2021-12-03
        响应：{
                "yesterday":{"11":383,"12":123,"17":88,"19":200 },
                "today":{"12":38,"13":1233,"17":123,"19":688 }
                }
     */

    /**
     * 日活分时统计
     * @param id
     * @param dt
     * @return
     */
    @RequestMapping("/realtimeHour")
    public Object realtimeHour(String id, @RequestParam("date") String dt) {
        if (id.equals("dau")) {
            // 封装返回的数据
            HashMap<String, Map<String, Long>> hourMap = new HashMap<>();
            // 获取今日的日活分时统计
            Map<String,Long> tdHour = esService.getDauHour(dt);
            hourMap.put("today", tdHour);
            // 获取昨天的日活统计
            String yd = getYd(dt);
            Map<String,Long> ydHour = esService.getDauHour(yd);
            hourMap.put("yesterday",ydHour);
            return hourMap;
        } else {
            return null;
        }
    }

    /**
     * 获取昨天日期
     * @param td
     * @return
     */
    private  String getYd(String td){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yd = null;
        try {
            Date tdDate = dateFormat.parse(td);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            yd = dateFormat.format(ydDate);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式转变失败");
        }
        return yd;
    }

}
