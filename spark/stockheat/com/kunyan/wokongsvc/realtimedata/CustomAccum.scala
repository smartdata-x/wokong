/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/CustomAccum.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-05-23 16:22
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import org.apache.spark.AccumulatorParam
import org.apache.spark.Accumulator

/**
  * Created by wukun on 2016/5/19
  * 自定义字符串类型累加器
  */
object CustomAccum {

  implicit object StringAccumulatorParam extends AccumulatorParam[String] {

    /**
      * 实现字符串类型相加
      * @param t1 被加字符串
      * @param t2 加字符串
      * @author wukun
      */
    def addInPlace(
      t1: String, 
      t2: String ): String = (new StringBuilder(t1).toLong + new StringBuilder(t2).toLong).toString

    /**
      * 第一个字符串赋初值
      * @param initialValue 初始字符串值
      * @author wukun
      */
    def zero(initialValue: String): String = "0"
  }
}
