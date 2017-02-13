/*=============================================================================
# Copyright (c) 2015
# ShanghaiKunyan.  All rights reserved
# Filename : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/spark_kafka/src/main/scala/SparkKafka.scala
# Author   : Sunsolo
# Email    : wukun@kunyan-inc.com
# Date     : 2016-05-17 21:34
=============================================================================*/
package com.kunyan.wokongsvc.realtimedata

import java.sql.Connection
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
  * Created by wukun on 2016/5/23
  * kafka数据操作主程序入口
  */
object SparkVisit {

  def main(args: Array[String]) {

    var NEEDFILTER = false
    var LEVEL: Int = 6

    if (args.length < 2) {
      System.exit(-1)
    }

    if (args.length == 3) {
      NEEDFILTER = true
      LEVEL = args(2).toInt
    }

    /* 加载日志配置文件 */
    PropertyConfigurator.configure(args(0))

    val xmlHandle = XmlHandle(args(1))

    /* 初始化mysql连接池 */
    val execPool = MysqlPool(xmlHandle)
    val otherPool = MysqlPool(xmlHandle, "other_stock")
    val changePool = MysqlPool(xmlHandle, "change")

    /* 初始化mysql连接池 */
    val masterPool = MysqlPool(xmlHandle)
    masterPool.setConfig(1, 1, 3)
    Stock.initStockAlias(masterPool)

    /* 初始化kafka生产者 */
    val kafkaProducer = KafkaProducer(xmlHandle)

    val alias = Stock.stockAlias

    /* 初始化spark运行上下文环境 */
    val sparkConf = new SparkConf().setAppName("visitHeat")
    val spc = new SparkContext(sparkConf)
    val stc = new StreamingContext(spc, Seconds(60))

    var prevRdd: RDD[((String, Long), Int)] = null

    /* 广播连接池、股票和到行业及概念的映射 */
    val otherStockPoolBr = stc.sparkContext.broadcast(otherPool)
    val stockPoolBr = stc.sparkContext.broadcast(execPool)
    val changePoorBr = stc.sparkContext.broadcast(changePool)

    /* 初始化计算最大股票访问量；累加器 */
    val accum = stc.sparkContext.accumulator[(String, Int)](("0", 0))
    val isAlarm = stc.sparkContext.accumulator[Int](0)
    val heatInfo = stc.sparkContext.accumulator[List[StockInfo]](Nil)
    var isToHour: Long = 0L
    var lastUpdateTime = 0

    /* 初始化kafka参数并创建Dstream对象 */
    val kafkaParam = Map("metadata.broker.list" -> xmlHandle.getElem("kafka", "broker"), "group.id" -> "visit")
    val topicParam = xmlHandle.getElem("kafka", "topic")

    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](stc, kafkaParam, topicParam.split(",").toSet).filter(_._2.nonEmpty)
      .foreachRDD(rows => {

        val currCal: Calendar = Calendar.getInstance
        val nowUpdateTime = TimeHandle.getDay
        val currHour = TimeHandle.getNowHour(currCal)
        val currTimeStamp = currCal.getTimeInMillis

        if (nowUpdateTime != lastUpdateTime && currHour == 0) {

          RddOpt.updateAccum(stockPoolBr.value.getConnect, "stock_visit", 0)
          RddOpt.updateAccum(otherStockPoolBr.value.getConnect, "stock_visit", 0)
          RddOpt.updateOtherAccum(changePoorBr.value.getConnect, "stock_visit")

          Stock.initStockAlias(masterPool)
          lastUpdateTime = nowUpdateTime
        }

        val currYear = TimeHandle.getYear(currCal)
        val currMonth = TimeHandle.getMonth(currCal, 1)
        val currDay = TimeHandle.getDay(currCal)
        val currStamp = TimeHandle.getStamp(currCal)

        val eachCodeCount = rows
          .map(row => row._2)
          .map(x => MixTool.stockClassify(x, alias, NEEDFILTER, LEVEL, currStamp))
          .filter(x => {
            isAlarm += 1

            if (x._1._2.compareTo("0") == 0 || x._1._2.compareTo("2") == 0) {
              false
            } else {
              true
            }
            //因为此次要做累加统计，因此在此之前时间戳必须要转为分钟时间戳,先stock_type中插入数据时也需要判断之前有没有主键
          }).map(cell => ((cell._1._1, cell._2), 1)).reduceByKey(_ + _)
          .coalesce(3) //重新分区
          .persist(StorageLevel.MEMORY_ONLY)

        if (!eachCodeCount.isEmpty()) {

          eachCodeCount.foreachPartition(x_ => {

            val x = x_.toList

            stockPoolBr.value.getConnect match {

              case Some(connect) =>

                val stockHandle = MysqlHandle(connect)

                RddOpt.updateStocksCount(stockHandle, x, accum, heatInfo, MixTool.VISIT, currTimeStamp)

                stockHandle.batchExec recover {
                  case e: Exception =>
                    exception(e)
                }

                stockHandle.close()

              case None => logger.warn("Get connect exception")
            }

            otherStockPoolBr.value.getConnect match {

              case Some(conn) =>

                val otherStockHandle = MysqlHandle(conn)

                RddOpt.updateOtherStockCount(otherStockHandle, x, MixTool.VISIT, currStamp)

                otherStockHandle.batchExec recover {
                  case e: Exception =>
                    exception(e)
                }

                otherStockHandle.close()

              case None => logger.warn("Get connect exception")
            }

            changePoorBr.value.getConnect match {

              case Some(conn) =>

                val changePoolHandle = MysqlHandle(conn)

                RddOpt.updateChange(changePoolHandle, x, MixTool.VISIT, currStamp)

                changePoolHandle.batchExec recover {
                  case e: Exception =>
                    exception(e)
                }

                changePoolHandle.close()

              case None => logger.warn("change pool connect exception")
            }

          })

          //todo add表有没有必要
          if (prevRdd == null) {

            eachCodeCount.foreachPartition(y => {

              stockPoolBr.value.getConnect match {

                case Some(connect) =>

                  val mysqlHandle = MysqlHandle(connect)

                  RddOpt.updateAddFirst(mysqlHandle, y, "stock_visit_add", currStamp)

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

              stockPoolBr.value.getConnect match {

                case Some(connect) =>

                  val mysqlHandle = MysqlHandle(connect)

                  RddOpt.updateAdd(mysqlHandle, y, "stock_visit_add", currStamp)

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

          val count = eachCodeCount.map(y => (y._1._2, y._2)).reduceByKey(_ + _).foreach(x =>
            RddOpt.updateTotal(stockPoolBr.value.getConnect, MixTool.ALL_VISIT, x))


          val maxTime = eachCodeCount.map(_._1._2).reduce(_ max _)

          RddOpt.updateTime(masterPool.getConnect, "update_visit", maxTime)

          accum.setValue(("0", 0))

          if (isAlarm.value == 0) {

            if ((isToHour % 12) == 0) {
              TextSender.send("C4545CC1AE91802D2C0FBA7075ADA972", "The real visit data is exception", "17839613797")
            }

            isToHour += 1
          }

          isAlarm.setValue(0)

          val stockInfo = heatInfo.value

          //这边有一个问题，我这边发送的时间应该怎么发送
          import JsonHandle.MyJsonProtocol._
          val json = MixData("visit", currStamp, currMonth, currDay, currHour, stockInfo).toJson.compactPrint

          kafkaProducer.send("0", json)

          heatInfo.setValue(Nil)
        }
      })

    stc.start
    stc.awaitTermination
    stc.stop()
  }

}
