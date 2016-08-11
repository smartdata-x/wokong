/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/FileContext.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-06-01 18:33
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import java.util.Calendar

import org.apache.spark.SparkConf 
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.AccumulatorParam

/**
  * Created by wukun on 2016/06/01
  * 自定义静态文件相关的上下文类
  */
class FileContext(xml: XmlHandle) {

  private val sparkContext = new SparkContext(new SparkConf().setAppName("searchHeat"))

  private val mysqlPool = MysqlPool(xml)

  /**
    * 获取要操作文件的时间戳
    * @author wukun
    */
  def getTamp: String = {

    val cal = Calendar.getInstance

    val year = TimeHandle.getYear(cal)
    val month = TimeHandle.getMonth(cal)
    var day = TimeHandle.getDay(cal)
    var hour = 0

    if(TimeHandle.getZeHour(cal).toInt >= 0 && TimeHandle.getZeHour(cal).toInt <= 4) {

      hour = TimeHandle.getZeHour(cal).toInt + 23 + 1 - 5
      day = day - 1

    } else {
      hour = TimeHandle.getZeHour(cal).toInt - 5
    }

    var strHour = ""

    if(hour >= 0 && hour <= 9) {
      strHour = "0" + hour
    } else {
      strHour = "" + hour
    }

    var strDay = ""

    if(day <= 9) {
      strDay = "0" + day
    } else {
      strDay = day.toString
    }

    year + month + strDay + strHour
  }

  def getFileName: String = {
    xml.getElem("file", "path") + "shdx_" + getTamp
  }

  def generateRdd: RDD[String] = {
    sparkContext.textFile(getFileName)
  }

  def broadcastPool: Broadcast[MysqlPool] = {
    sparkContext.broadcast(mysqlPool)
  }

  def broadcastSource[T: ClassTag](source: T) = {
    sparkContext.broadcast[T](source)
  }

  def accum[T: ClassTag](initVal: T)(implicit param: AccumulatorParam[T]) = {
    sparkContext.accumulator[T](initVal)
  }

}

/**
  * Created by wukun on 2016/06/01
  * FileContext伴生对象
  */
object FileContext {

  def apply(xml:XmlHandle): FileContext = {
    new FileContext(xml)
  }
}

