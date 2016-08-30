/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/JsonHandle.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-08-18 13:35
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import scala.collection.mutable.ArrayBuffer

import spray.json._
import DefaultJsonProtocol._ 
import scala.collection.immutable
import scala.reflect.ClassTag

/**
  * Created by wukun on 2016/08/18
  * json操作句柄
  */
class JsonHandle {
}

/**
  * Created by wukun on 2016/08/18
  * json句柄伴生对象
  */
object JsonHandle {

  /** 把要发送的json格式定义成伴生类，这样要发送数据只要初始化类就好 */
  case class MixData(stamp: Long = 0, month: Int, day: Int, stock: List[StockInfo])
  case class StockInfo(code: String, value: Int)

  object MyJsonProtocol extends DefaultJsonProtocol {
    /** 当定义好类格式后，要对类进行json协议转化 ,这个地方很重要*/
    implicit val stockInfo = jsonFormat2(StockInfo)
    implicit val mixData = jsonFormat4(MixData)
  }

  /** 在使用上面得转化格式时，要显示调用下面的语句，进行导入 */
  import JsonHandle.MyJsonProtocol._

  def toString(value: MixData): String = {
    value.toJson.compactPrint
  }

  def apply(): JsonHandle = {
    new JsonHandle
  }
}
