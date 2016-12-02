/*=============================================================================
# Copyright (c) 2015
# ShanghaiKunyan.  All rights reserved
# Filename : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/HeatThread.scala
# Author   : Sunsolo
# Email    : wukun@kunyan-inc.com
# Date     : 2016-08-25 07:44
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import com.kunyan.wokongsvc.realtimedata.JsonHandle.{MixData, StockInfo}
import com.kunyan.wokongsvc.realtimedata.logger.HeatLogger
import kafka.consumer.KafkaStream
import spray.json._

import scala.collection.mutable

/**
  * Created by wukun on 2016/08/25
  * 热度线程类
  */
class HeatThread(
                  val stream: KafkaStream[Array[Byte], Array[Byte]],
                  val pool: MysqlPool,
                  val rank: Int
                ) extends Runnable {

  var stock_type: String = _
  var (month: Int, day: Int, hour: Int) = TimeHandle.getMonthDayHour
  val codeCount = mutable.HashMap[String, Int]()

  def timeCompute(body: => Unit) {
    val start = System.currentTimeMillis
    body
    val end = System.currentTimeMillis
  }

  def doWork(stockInfos: List[StockInfo]) {
    stockInfos.foreach(x => {
      val initialVal = codeCount.applyOrElse(x.code, (y: String) => 0)
      codeCount += ((x.code, x.value + initialVal))
    })
  }

  /**
    * 用统计的股票热度数据来更新查看月份表
    */
  def mysqlOpt {

    pool.getConnect match {

      case Some(connect) => {

        val sqlHandle = MysqlHandle(connect)

        codeCount.foreach(x => {
          sqlHandle.addCommand(
            MixTool.updateMonthAccum("stock_" + stock_type + "_month_", x._1, month, day, x._2)
          ) recover {
            case e: Exception => HeatLogger.exception(e)
          }
        })

        sqlHandle.batchExec recover {
          case e: Exception => {
            HeatLogger.exception(e)
          }
        }
        sqlHandle.close
      }
      case None => {
        HeatLogger.error("[Get mysql connect failure]")
      }
    }
  }

  /**
    * 重载的可运行任务类中的run方法
    */
  override def run {

    val iter = stream.iterator

    while (iter.hasNext) {

      import JsonHandle.MyJsonProtocol._
      val json = new String(iter.next.message).parseJson.convertTo[MixData]
      stock_type = json.stock_type
      val stamp = json.stamp
      val nowMonth = json.month
      val nowDay = json.day
      val nowHour = json.hour
      val stockInfos = json.stock

      if (nowDay != day) {
        timeCompute(mysqlOpt)
        codeCount.clear
        month = nowMonth
        day = nowDay
      }

      doWork(stockInfos)
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
             pool: MysqlPool,
             rank: Int
           ): HeatThread = {
    new HeatThread(stream, pool, rank)
  }
}

