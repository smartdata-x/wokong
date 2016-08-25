/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/HeatThread.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-08-25 07:44
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import JsonHandle._
import JsonHandle.MyJsonProtocol._

import spray.json._
import DefaultJsonProtocol._ 

import kafka.consumer.KafkaStream

/**
  * Created by wukun on 2016/08/25
  * 热度线程类
  */
class HeatThread(
  val stream: KafkaStream[Array[Byte], Array[Byte]], 
  val pool: MysqlPool
) extends Runnable with CustomLogger {

  /**
    * 用统计的股票热度数据来更新查看月份表
    * @param month  当前的月份
    * @param day    当前的天
    * @param stocks 要更新的股票集合
    */
  def doWork(month: Int, day: Int, stocks: List[StockInfo]) {

    pool.getConnect match {

      case Some(connect) => {

        val sqlHandle = MysqlHandle(connect)
        println(System.currentTimeMillis)

        stocks.foreach( x => {
          sqlHandle.addCommand(
            MixTool.updateMonthAccum("stock_visit_month_", x.code, month, day, x.value)
          ) recover {
            case e: Exception => warnLog(fileInfo, e.getMessage)
          } 
        })

        sqlHandle.batchExec recover {
          case e: Exception => {
            warnLog(fileInfo, "[exec updateAdd failure]" + e.getMessage)
          }
        }

        sqlHandle.close
        println(System.currentTimeMillis)
      }

      case None => {
        warnLog(fileInfo, "[Get mysql connect failure]")
      }
    }
  }

  /**
    * 重载的可运行任务类中的run方法
    */
  override def run {

    val iter = stream.iterator

    while(iter.hasNext) {

      val json = (new String(iter.next.message)).parseJson.convertTo[MixData]
      val month = json.month
      val day = json.day
      val stockInfos = json.stock

      doWork(month, day, stockInfos)
    }
  }
}

/**
  * Created by wukun on 2016/08/25
  * 热度线程类伴生对象
  */
object HeatThread {

  def apply(
    stream: KafkaStream[Array[Byte], Array[Byte]], 
    pool: MysqlPool
  ): HeatThread = {
    new HeatThread(stream, pool)
  }
}

