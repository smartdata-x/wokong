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
import scala.collection.mutable.HashSet

/**
  * Created by wukun on 2016/5/19
  * 实时数据中股票匹配模式类
  */
object DataPattern {

  type Tuple2Map = (HashSet[String], (Map[String, String], Map[String, String], Map[String, String]))

  val DIGITPATTERN = "([0-9]{6})".r
  val ENCODEPATTERN = "(\\*[A-Za-z0-9_]*%.*)".r
  val ALPHAPATTERN = "(\\*[a-zA-Z]*)".r

  /**
    * 根据正则模式解析出不同格式的股票字符串
    * @param stockStr 要解析的字符串
    * @author wukun
    */
  def stockCodeMatch(stockStr: String, alias: Tuple2Map): String = {

    stockStr match {
      case DIGITPATTERN(first) => if(alias._1(first)) first else "0"
      case ENCODEPATTERN(_*) => alias._2._1.getOrElse(stockStr, "0")
      case ALPHAPATTERN(first) => {
        val stock = first.toUpperCase
        alias._2._2.getOrElse(stock, alias._2._3.getOrElse(stock, "0"))
      }
      case _ => "0"
    }
  }

}
