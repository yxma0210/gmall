package com.myx.gmall.realtime.bean

/**
 * @Description: 用于映射用户状态表的样例类
 * @author: mayx
 * @date: 2021/12/11 15:58
 */
case class UserStatus(
                       userId:String,  //用户id
                       ifConsumed:String //是否消费过   0首单   1非首单
                     )
