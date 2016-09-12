/*=============================================================================
# Copyright (c) 2015
# ShanghaiKunyan.  All rights reserved
# Filename : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/HeatThread.scala
# Author   : Sunsolo
# Email    : wukun@kunyan-inc.com
# Date     : 2016-08-25 07:44
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.Timer
import com.codahale.metrics.Timer.Context
import java.util.concurrent.TimeUnit
import JsonHandle._
import JsonHandle.MyJsonProtocol._

import spray.json._
import DefaultJsonProtocol._ 

import kafka.consumer.KafkaStream
import scala.collection.mutable

/**
  * Created by wukun on 2016/08/25
  * 热度线程类
  */
class HeatThread(
  val stream: KafkaStream[Array[Byte], Array[Byte]], 
  val pool: MysqlPool,
  val rank: Int
) extends Runnable with CustomLogger {

  val metrics = new MetricRegistry
  val report = ConsoleReporter.forRegistry(metrics).build()
  val costTime = metrics.timer(MetricRegistry.name(classOf[HeatThread], "costTime"))

  var stock_type: String = _
  var (month: Int, day: Int, hour: Int) = TimeHandle.getMonthDayHour
  val codeCount = mutable.HashMap[String, Int]()

  def timeCompute(body: => Unit) {
    val start = System.currentTimeMillis
    body
    val end   = System.currentTimeMillis
  }

  def doWork(stockInfos: List[StockInfo]) {
    val context = costTime.time
    stockInfos.foreach( x => {
      val initialVal = codeCount.applyOrElse(x.code, (y: String) => 0)
      codeCount += ((x.code, x.value + initialVal))
    })
    context.stop
  }

  /**
    * 用统计的股票热度数据来更新查看月份表
    * @param month  当前的月份
    * @param day    当前的天
    * @param stocks 要更新的股票集合
    */
  def mysqlOpt {

    pool.getConnect match {

      case Some(connect) => {

        val sqlHandle = MysqlHandle(connect)

        codeCount.foreach( x => {
          sqlHandle.addCommand(
            MixTool.updateMonthAccum("stock_" + stock_type + "_month_", x._1, month, day, x._2)
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

    report.start(10, TimeUnit.SECONDS)

    val iter = stream.iterator

    while(iter.hasNext) {

      val json = (new String(iter.next.message)).parseJson.convertTo[MixData]
      stock_type = json.stock_type
      val stamp = json.stamp
      val nowMonth = json.month
      val nowDay = json.day
      val stockInfos = json.stock

      if(nowDay != day) {
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

