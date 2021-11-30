package com.myx.gmall.realtime.bean

/**
 * @Description: 封装日活数据的样例类
 * @author: mayx
 * @date: 2021/11/30 17:32
 */
case class DauInfo(
                    mid:String,//设备id
                    uid:String,//用户id
                    ar:String,//地区
                    ch:String,//渠道
                    vc:String,//版本
                    var dt:String,//日期
                    var hr:String,//小时
                    var mi:String,//分钟
                    ts:Long //时间戳
                  ) {}
