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
 import java.text.SimpleDateFormat

 /**
   * Created by wukun on 2016/5/23
   * 时间操作相关类
   */
 object TimeHandle {

   def minStamp(interval:Long): Long = {
     System.currentTimeMillis - interval
   }

   def maxStamp: Long = {
     System.currentTimeMillis
   }

   def getYear(cal: Calendar): String = {
     cal.get(Calendar.YEAR).toString
   }

   /**
     * 得到指定时间所属的月份(从0开始)
     * @param cal 要操作的时间类
     * @auhtor wukun
     */
   def getMonth(cal: Calendar): String = {

     val month = cal.get(Calendar.MONTH) + 1
     if(month <= 9) {
       "0" + month
     } else {
       month.toString
     }
   }

   def getMonth(cal: Calendar, add: Int): Int = {

     val month = cal.get(Calendar.MONTH) + add

     month
   }

   def getDay(cal: Calendar): Int = {

     val day = cal.get(Calendar.DAY_OF_MONTH)

     day
   }

   def getDay: Int = {
     val cal = Calendar.getInstance
     val day = cal.get(Calendar.DAY_OF_MONTH)

     day
   }

   /**
     * 得到指定时间所属的小时
     * @param cal 要操作的时间类
     * @auhtor wukun
     */
   def getZeHour(cal: Calendar): String = {

     val hour = cal.get(Calendar.HOUR_OF_DAY)

     if(hour <= 9) {
       "0" + hour
     } else {
       hour.toString
     }
   }

   def getHour(cal: Calendar): Int = {
     cal.get(Calendar.HOUR_OF_DAY) 
   }

   def getNowHour(cal: Calendar): Int = {
     val hour = cal.get(Calendar.HOUR_OF_DAY)
     hour
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

   def getTamp(str: String): Date = {
     val simpleDateFormat = new SimpleDateFormat("yyyyMMddHH")
     val date = simpleDateFormat.parse(str)

     date
   }
 }
