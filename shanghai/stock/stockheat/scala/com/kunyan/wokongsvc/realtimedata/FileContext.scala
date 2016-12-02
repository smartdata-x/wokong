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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{AccumulatorParam, SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * Created by wukun on 2016/06/01
  * 自定义静态文件相关的上下文类
  */
class FileContext(xml: XmlHandle) {

  private val sparkContext = new SparkContext(new SparkConf().setAppName("searchHeat"))

  private val mysqlPool = MysqlPool(xml)

  val basicPath = xml.getElem("file", "path")

  /**
    * 获取要操作文件的时间戳
    *
    * @author wukun
    */
  def getTamp: String = {

    val cal = Calendar.getInstance

    val year = TimeHandle.getYear(cal)
    val month = TimeHandle.getMonth(cal)
    var day = TimeHandle.getDay(cal)
    var hour = 0

    if(TimeHandle.getZeHour(cal).toInt >= 0 && TimeHandle.getZeHour(cal).toInt <= 3) {
      hour = TimeHandle.getZeHour(cal).toInt + 23 + 1 - 4
      day = day - 1
    } else {
      hour = TimeHandle.getZeHour(cal).toInt - 4
    }

    val strHour = {

      if(hour >= 0 && hour <= 9) {
        "0" + hour
      } else {
        "" + hour
      }

    }

    val strDay = {

      if(day <= 9) {
        "0" + day
      } else {
        day.toString
      }

    }

    year + month + strDay + strHour
  }

  def getFilePath(fileName: String): String = {
    basicPath + fileName
  }

  def getLazyFile(path: String): String = {

    val fileHandle = FileHandle(path)
    val reader = fileHandle.initBuff()

    var (delete, ident, save) = (reader.readLine(), reader.readLine(), "")

    while(ident != null) {
      save += ident + "\n"
      ident = reader.readLine()
    }

    reader.close()

    // writer和reader不可同时使用，否则会出异常
    // 所以将这俩的初始化分开
    val writer = fileHandle.initWriter()
    writer.write(save.trim) 
    writer.close()

    delete
  }

  def generateRdd(fileName: String): RDD[String] = {
    sparkContext.textFile(getFilePath(fileName))
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

