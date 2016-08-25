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

import JsonHandle.StockInfo

import org.apache.spark.AccumulatorParam
import org.apache.spark.Accumulator

import scala.collection.mutable.HashMap

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

  implicit object HashMapAccumulatorParam extends AccumulatorParam[HashMap[String, Int]] {

    /**
      * 实现HashMap类型相加
      * @param t1 被加HashMap
      * @param t2 加HashMap
      * @author wukun
      */
    def addInPlace(
      t1: HashMap[String, Int],
      t2: HashMap[String, Int]
    ): HashMap[String, Int] = {

      val keys = t2.keys

      for(key <- keys) {

        if(!t1.contains(key)) {
          t1 += ((key, t2(key)))
        }

      }

      t1
    }

    /**
      * 第一个HashMap赋初值
      * @param initialValue 初始HashMap值
      * @author wukun
      */
    def zero(initialValue: HashMap[String, Int]): HashMap[String, Int] = initialValue
  }

  implicit object StringIntAccumulatorParam extends AccumulatorParam[(String, Int)] {

    /**
      * 实现元组类型相加
      * @param t1 被加元组
      * @param t2 加元组
      * @author wukun
      */
    def addInPlace(
      t1: (String, Int),
      t2: (String, Int)
    ): (String, Int) = {

      if(t2._2 > t1._2) {
        t2
      } else {
        t1
      }
    }

    /**
      * 第一个元组赋初值
      * @param initialValue 初始元组值
      * @author wukun
      */
    def zero(initialValue: (String, Int)): (String, Int) = ("0", 0)
  }

  implicit object ListObjectAccumulatorParam extends AccumulatorParam[List[StockInfo]] {

    /**
      * 实现List类型相加
      * @param t1 被加List
      * @param t2 加List
      * @author wukun
      */
    def addInPlace(
      t1: List[StockInfo],
      t2: List[StockInfo]
    ): List[StockInfo] = {
      t1 ::: t2
    }

    /**
      * 第一个元组赋初值
      * @param initialValue 初始元组值
      * @author wukun
      */
    def zero(initialValue: List[StockInfo]): List[StockInfo] = Nil
  }
}



