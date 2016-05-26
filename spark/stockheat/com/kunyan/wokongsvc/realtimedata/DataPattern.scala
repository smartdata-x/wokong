/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/DataPattern.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-05-19 07:57
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import scala.util.matching.Regex
import scala.collection.mutable.HashMap

/**
  * Created by wukun on 2016/5/19
  * 实时数据中股票匹配模式类
  */
object DataPattern {

  val digitPattern = "([0-9]{6})".r
  val encodePattern = "((%.*){8})".r
  val alphaPattern = "([a-zA-Z]*)".r

  /**
    * 根据正则模式解析出不同格式的股票字符串
    * @param stockStr 要解析的字符串
    * @author wukun
    */
  def stockCodeMatch(stockStr: String): String = {

    stockStr match {
      case digitPattern(first) => first
      case encodePattern(_*) => stockStr
      case alphaPattern(first) => first.toUpperCase
      case _ => "0"
    }
  }
}
