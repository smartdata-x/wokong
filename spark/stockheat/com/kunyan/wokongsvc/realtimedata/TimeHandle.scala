/*=============================================================================
  #    Copyright (c) 2015
  #    ShanghaiKunyan.  All rights reserved
  #
  #    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/TimeHandle.scala
  #    Author       : Sunsolo
  #    Email        : wukun@kunyan-inc.com
  #    Date         : 2016-05-22 15:16
  #    Description  : 
  =============================================================================*/

 package com.kunyan.wokongsvc.realtimedata

 import java.util.Calendar
 import java.util.Date

 /**
   * Created by wukun on 2016/5/23
   * 时间操作相关类
   */
 object TimeHandle {

   def minStamp(interval:Long):Long = {
     System.currentTimeMillis - interval
   }

   def maxStamp:Long = {
     System.currentTimeMillis
   }

   /**
     * 将当前时间以分钟为单位延后
     * @param cal 要操作的时间类
     * @param minute 分钟....
     * @auhtor wukun
     */
   def setTime(cal:Calendar, minute:Int, second:Int, milliSecond:Int) {
     cal.add(Calendar.MINUTE, minute)
     cal.set(Calendar.SECOND, second)
     cal.set(Calendar.MILLISECOND, milliSecond)
   }

   /**
     * 将当前时间以小时为单位延后
     * @param cal 要操作的时间类
     * @param hour 小时....
     * @auhtor wukun
     */
   def setTime(cal:Calendar, hour:Int, minute:Int, second:Int, milliSecond:Int) {
     cal.set(Calendar.HOUR_OF_DAY, hour)
     cal.set(Calendar.MINUTE, minute)
     cal.set(Calendar.SECOND, second)
     cal.set(Calendar.MILLISECOND, milliSecond)
   }
 }
