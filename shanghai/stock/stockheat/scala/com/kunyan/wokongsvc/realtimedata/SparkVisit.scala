/*=============================================================================
# Copyright (c) 2015
# ShanghaiKunyan.  All rights reserved
# Filename : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/spark_kafka/src/main/scala/SparkKafka.scala
# Author   : Sunsolo
# Email    : wukun@kunyan-inc.com
# Date     : 2016-05-17 21:34
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
    val testPool = MysqlPool(xmlHandle, "test")
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

    var prevRdd: RDD[((String, String), Int)] = null

    /* 广播连接池、股票和到行业及概念的映射 */
    val otherStockPoolBr = stc.sparkContext.broadcast(otherPool)
    val stockPoolBr = stc.sparkContext.broadcast(execPool)
    val testPoolBr = stc.sparkContext.broadcast(testPool)

    /* 初始化计算最大股票访问量；累加器 */
    val accum = stc.sparkContext.accumulator[(String, Int)](("0", 0))
    val isAlarm = stc.sparkContext.accumulator[Int](0)
    val heatInfo = stc.sparkContext.accumulator[List[StockInfo]](Nil)
    var isToHour: Long = 0L
    var lastUpdateTime = 0

    /* 初始化kafka参数并创建Dstream对象 */
    val kafkaParam = Map("metadata.broker.list" -> xmlHandle.getElem("kafka", "broker"), "group.id" -> "visit")
    val topicParam = xmlHandle.getElem("kafka", "topic")
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](stc, kafkaParam, topicParam.split(",").toSet)
      .foreachRDD(rows => {

        val cal: Calendar = Calendar.getInstance
        val nowUpdateTime = TimeHandle.getDay
        val hour = TimeHandle.getNowHour(cal)

        if (nowUpdateTime != lastUpdateTime && hour == 0) {
          RddOpt.updateAccum(masterPool.getConnect, otherPool.getConnect,"stock_visit", 0)
          Stock.initStockAlias(masterPool)
          lastUpdateTime = nowUpdateTime
        }

        val year = TimeHandle.getYear(cal)
        val month = TimeHandle.getMonth(cal, 1)
        val day = TimeHandle.getDay(cal)
        val stamp = TimeHandle.getStamp(cal)

        val eachCodeCount = rows
          .map(row => row._2)
          .map(x => MixTool.stockClassify(x, alias, NEEDFILTER, LEVEL))
          .filter(x => {
            isAlarm += 1

            if (x._1._2.compareTo("0") == 0 || x._1._2.compareTo("2") == 0) {
              false
            } else {
              true
            }
          })
          .map((y: ((String, String), String)) => (y._1, 1))
          .reduceByKey(_ + _)
          .coalesce(3) //重新分区
          .persist(StorageLevel.MEMORY_ONLY)

        eachCodeCount.foreachPartition(x_ => {

          val x = x_.toList

          stockPoolBr.value.getConnect match {

            case Some(connect) =>

              val stockHandle = MysqlHandle(connect)

              RddOpt.updateStocksCount(stockHandle, x, accum, heatInfo, MixTool.VISIT, stamp, month, day)

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

              RddOpt.updateOtherStockCount(otherStockHandle, x, MixTool.VISIT, year, month, stamp)

              otherStockHandle.batchExec recover {
                case e: Exception =>
                  exception(e)
              }

              otherStockHandle.close()

            case None => logger.warn("Get connect exception")
          }

          testPoolBr.value.getConnect match {

            case Some(conn) =>

              val testHandle = MysqlHandle(conn)

              RddOpt.updateOtherStockCount(testHandle, x, MixTool.VISIT, year, month, stamp)

              testHandle.batchExec recover {
                case e: Exception =>
                  exception(e)
              }

              testHandle.close()

            case None => logger.warn("Get connect exception")
          }

        })

        if (prevRdd == null) {

          eachCodeCount.map(x => (x._1._1, x._2)).foreachPartition(y => {

            stockPoolBr.value.getConnect match {

              case Some(connect) =>

                val mysqlHandle = MysqlHandle(connect)

                RddOpt.updateAddFirst(mysqlHandle, y, "stock_visit_add", stamp)

                mysqlHandle.batchExec recover {
                  case e: Exception =>
                    exception(e)
                }

                mysqlHandle.close()

              case None => logger.warn("Get connect exception")
            }
          })

        } else {

          eachCodeCount.fullOuterJoin[Int](prevRdd).map(x => (x._1._1, x._2)).foreachPartition(y => {

            stockPoolBr.value.getConnect match {

              case Some(connect) =>

                val mysqlHandle = MysqlHandle(connect)

                RddOpt.updateAdd(mysqlHandle, y, "stock_visit_add", stamp)

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

        eachCodeCount.map(y => {
          (y._1._2, y._2)
        }).reduceByKey(_ + _).foreach(z => {
          RddOpt.updateTotal(stockPoolBr.value.getConnect, MixTool.ALL_VISIT, stamp, z._2)
        })

        RddOpt.updateTime(masterPool.getConnect, "update_visit", stamp)

        accum.setValue(("0", 0))

        if (isAlarm.value == 0) {

          if ((isToHour % 12) == 0) {
            TextSender.send("C4545CC1AE91802D2C0FBA7075ADA972", "The real visit data is exception", "17839613797")
          }

          isToHour += 1
        }

        isAlarm.setValue(0)

        val stockInfo = heatInfo.value

        import JsonHandle.MyJsonProtocol._
        val json = MixData("visit", stamp, month, day, hour, stockInfo).toJson.compactPrint

        kafkaProducer.send("0", json)

        heatInfo.setValue(Nil)
      })

    stc.start
    stc.awaitTermination
    stc.stop()
  }

}
