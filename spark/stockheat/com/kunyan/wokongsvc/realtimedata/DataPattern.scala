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
import scala.collection.mutable.Map

/**
  * Created by wukun on 2016/5/19
  * 实时数据中股票匹配模式类
  */
object DataPattern {

   type Tuple3Map = (Map[String, String], Map[String, String], Map[String, String])

  val DIGITPATTERN = "([0-9]{6})".r
  val ENCODEPATTERN = "((%.*){8})".r
  val ALPHAPATTERN = "([a-zA-Z]*)".r

  /**
    * 根据正则模式解析出不同格式的股票字符串
    * @param stockStr 要解析的字符串
    * @author wukun
    */
  def stockCodeMatch(stockStr: String, alias: Tuple3Map): String = {

    stockStr match {
      case DIGITPATTERN(first) => first
      case ENCODEPATTERN(_*) => alias._1.getOrElse(stockStr, "0")
      case ALPHAPATTERN(first) => {
        val stock = first.toUpperCase
        alias._2.getOrElse(stock, alias._3.getOrElse(stock, "0"))
      }
      case _ => "0"
    }
  }
}
