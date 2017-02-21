/*=============================================================================
# Copyright (c) 2015
# ShanghaiKunyan.  All rights reserved
#
# Filename : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/SparkFile.scala
# Author   : Sunsolo
# Email    : wukun@kunyan-inc.com
# Date     : 2016-06-01 20:51
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import java.util.Calendar

import com.kunyan.wokongsvc.realtimedata.CustomAccum._
import com.kunyan.wokongsvc.realtimedata.JsonHandle._
import kafka.serializer.StringDecoder
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._

/**
  * Created by wukun on 2016/06/01
  * 静态文件操作主入口
  */
object SparkSearch {

  var NEEDFILTER = false
  var LEVEL: Int = _

  def main(args: Array[String]) {

    logger.info("start execute")
    PropertyConfigurator.configure(args(0))

    if (args.length < 2) {
      logger.error("args too little")
      System.exit(-1)
    }

    if (args.length == 3) {
      NEEDFILTER = true
      LEVEL = args(2).toInt
    }

    val xmlHandle = XmlHandle(args(1))

    val execPool = MysqlPool(xmlHandle)
    val masterPool = MysqlPool(xmlHandle)
    //数据库连接池的设置
    masterPool.setConfig(1, 1, 3)
    Stock.initStockAlias(masterPool)

    val kafkaProducer = KafkaProducer(xmlHandle)

    val sparkConf = new SparkConf().setAppName("searchHeat")
    val spc = new SparkContext(sparkConf)
    val stc = new StreamingContext(spc, Seconds(60))

    var prevRdd: RDD[((String, Long), Int)] = null

    val stockPool = stc.sparkContext.broadcast(execPool)

    val alias = Stock.stockAlias

    /* 初始化计算最大股票访问量；累加器 */
    val accum = stc.sparkContext.accumulator[(String, Int)](("0", 0))
    val heatInfo = stc.sparkContext.accumulator[List[StockInfo]](Nil)
    var lastUpdateTime = 0

    /* 初始化kafka参数并创建Dstream对象 */
    val kafkaParam = Map("metadata.broker.list" -> xmlHandle.getElem("kafka", "broker"), "group.id" -> "search")
    val topicParam = xmlHandle.getElem("kafka", "topic")
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](stc, kafkaParam, topicParam.split(",").toSet).filter(_._2.nonEmpty)
      .foreachRDD(rows => {

        val cal: Calendar = Calendar.getInstance
        val nowUpdateTime = TimeHandle.getDay
        val hour = TimeHandle.getNowHour(cal)

        //如果是新的一天，将_accum表的值设为0，删除search，_add,_count表中的数据，删除updated_  search中前一天的数据
        if (nowUpdateTime != lastUpdateTime && hour == 0) {
          RddOpt.updateAccum(masterPool.getConnect, "stock_search", 0)
          Stock.initStockAlias(masterPool)
          lastUpdateTime = nowUpdateTime
        }

        val currMonth = TimeHandle.getMonth(cal, 1)
        val currDay = TimeHandle.getDay(cal)
        val currStamp = TimeHandle.getStamp(cal)

        val eachCodeCount = rows
          .map(row => row._2) //接到的kafka数据
          .map(x => MixTool.stockClassify(x, alias, NEEDFILTER, LEVEL, currStamp)) //搜索  x._1._2 =2  查看 x._1._2 =1 无用数据 = 0  返回的结果是（（股票代码，搜索查看类型），时间）
          .filter(x => {

          if (x._1._2.compareTo("0") == 0 || x._1._2.compareTo("1") == 0) {
            false
          } else {
            true
          }
        }).map(cell => ((cell._1._1, cell._2), 1)).reduceByKey(_ + _) //对股票代码进行累加操作
          .coalesce(3) //重新分区
          .persist(StorageLevel.MEMORY_AND_DISK_SER)

        if (!eachCodeCount.isEmpty()) {

          eachCodeCount.foreachPartition(x => {

            stockPool.value.getConnect match {

              case Some(connect) =>

                val stockHandle = MysqlHandle(connect)
                //_searce，_old,_accum表更新股票代码，时间，和此段s时间内的搜索数量，_month表更新多了一个天数，accum和heatinfo累加股票和搜索数量
                RddOpt.updateStocksCount(stockHandle, x.toList, accum, heatInfo, MixTool.SEARCH, currStamp)

                stockHandle.batchExec recover {
                  case e: Exception =>
                    exception(e)
                }

                stockHandle close()

              case None => logger.warn("Get connect exception")
            }
          })

          //程序重新运行会执行为null代码
          if (prevRdd == null) {

            eachCodeCount.foreachPartition(y => {

              stockPool.value.getConnect match {

                case Some(connect) =>

                  val mysqlHandle = MysqlHandle(connect)
                  //直接在_add中插入股票，时间，和计数
                  RddOpt.updateAddFirst(mysqlHandle, y, "stock_search_add", currStamp)

                  mysqlHandle.batchExec recover {
                    case e: Exception =>
                      exception(e)
                  }

                  mysqlHandle.close()

                case None => logger.warn("Get connect exception")
              }
            })

          } else {

            eachCodeCount.fullOuterJoin[Int](prevRdd).foreachPartition(y => {

              stockPool.value.getConnect match {

                case Some(connect) =>

                  val mysqlHandle = MysqlHandle(connect)
                  //直接在_add中插入股票，时间，和计数的差值
                  RddOpt.updateAdd(mysqlHandle, y, "stock_search_add", currStamp)

                  mysqlHandle.batchExec recover {
                    case e: Exception =>
                      exception(e)
                  }

                  mysqlHandle.close()

                case None => logger.warn("Get connect exception")
              }
            })
          }

          prevRdd = eachCodeCount

          //按照股票的查看和搜索类型，和时间进行计数，更新_count表
          val count = eachCodeCount.map(y => (y._1._2, y._2)).reduceByKey(_ + _).foreach(z => {
            RddOpt.updateTotal(stockPool.value.getConnect, MixTool.ALL_SEARCH, z)
          })

          val maxTime = eachCodeCount.map(_._1._2).reduce(_ max _)
          //更新update_search表时间
          RddOpt.updateTime(masterPool.getConnect, "update_search", maxTime)

          accum.setValue(("0", 0))

          val stockInfo = heatInfo.value

          import JsonHandle.MyJsonProtocol._
          val json = MixData("search", currStamp, currMonth, currDay, hour, stockInfo).toJson.compactPrint //toJson.compactPrint没用过

          val topic = xmlHandle.getElem("kafkaconsumer", "searchtopic")

          kafkaProducer.send(topic, "0", json)

          heatInfo.setValue(Nil) //重设发送的股票和对应的计数

        }
      })

    stc.start
    stc.awaitTermination
    stc.stop()
  }
}

