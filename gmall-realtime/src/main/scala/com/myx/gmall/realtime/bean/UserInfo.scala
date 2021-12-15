package com.myx.gmall.realtime.bean

/**
 * @Description: 用户样例类
 * @author: mayx
 * @date: 2021/12/15 11:55
 */
case class UserInfo(
                     id:String,
                     user_level:String,
                     birthday:String,
                     gender:String,
                     var age_group:String,  //年龄段
                     var gender_name:String //性别
                   )
