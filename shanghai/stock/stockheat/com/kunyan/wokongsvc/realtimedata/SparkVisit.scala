/*============================================================================= 
# Copyright (c) 2015
# ShanghaiKunyan.  All rights reserved
# Filename : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/spark_kafka/src/main/scala/SparkKafka.scala
# Author   : Sunsolo
# Email    : wukun@kunyan-inc.com
# Date     : 2016-05-17 21:34
=============================================================================*/
package com.kunyan.wokongsvc.realtimedata

import com.kunyan.scalautil.message.TextSender
import CustomAccum._
import JsonHandle._
import JsonHandle.MyJsonProtocol._
import MixTool.Tuple2Map

import spray.json._
import DefaultJsonProtocol._ 
import kafka.serializer.StringDecoder
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.rdd.RDD 
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

import java.util.Calendar
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Created by wukun on 2016/5/23
  * kafka数据操作主程序入口
  */
object SparkVisit extends CustomLogger {

  type TupleHashMap = (HashMap[String, ListBuffer[String]], HashMap[String, ListBuffer[String]])

  def main(args: Array[String]) {

    if(args.length != 2) {
      errorLog(fileInfo, "args too little")
      System.exit(-1)
    }

    /* 加载日志配置文件 */
    PropertyConfigurator.configure(args(0))

    val xmlHandle = XmlHandle(args(1))

    /* 初始化mysql连接池 */
    val execPool = MysqlPool(xmlHandle)
    val otherPool = MysqlPool(xmlHandle, false)

    /* 初始化mysql连接池 */
    val masterPool = MysqlPool(xmlHandle)
    masterPool.setConfig(1, 1, 3)

    /* 初始化kafka生产者 */
    val kafkaProducer = KafkaProducer(xmlHandle)

    /* 初始化股票名称的各种表示方式 */
    val stockalias = masterPool.getConnect match {

      case Some(connect) => {

        val sqlHandle = MysqlHandle(connect)

        val alias = sqlHandle.execQueryStockAlias(MixTool.SYN_SQL) match {
          case Success(z) => z
          case Failure(e) => {
            errorLog(fileInfo, e.getMessage + "[Query stockAlias exception]")
            System.exit(-1)
          }
        }

        sqlHandle.close

        alias
      }

      case None => {
        errorLog(fileInfo, "[Get mysql connect failure]")
        System.exit(-1)
      }
    }

    /* 初始化spark运行上下文环境 */
    val sparkConf = new SparkConf().setAppName("visitHeat")
    val spc = new SparkContext(sparkConf)
    val stc = new StreamingContext(spc, Seconds(60))

    var prevRdd: RDD[((String, String), Int)] = null

    /* 广播连接池、股票和到行业及概念的映射 */
    val stockPool = stc.sparkContext.broadcast(execPool)
    val testPool = stc.sparkContext.broadcast(otherPool)
    val alias = stc.sparkContext.broadcast(stockalias.asInstanceOf[Tuple2Map])

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
    .foreach( rows => {

      val cal: Calendar = Calendar.getInstance
      val nowUpdateTime = TimeHandle.getDay
      val hour = TimeHandle.getNowHour(cal)
      if(nowUpdateTime != lastUpdateTime && hour == 0) {
        RddOpt.updateAccum(masterPool.getConnect, "stock_visit", 0)
        lastUpdateTime = nowUpdateTime
      }

      val month = TimeHandle.getMonth(cal, 1)
      val day = TimeHandle.getDay(cal)
      val stamp = TimeHandle.getStamp(cal)

      val eachCodeCount = rows
      .map( row => row._2)
      .map( x => MixTool.stockClassify(x, alias.value))
      .filter( x => {
        isAlarm += 1

        if(x._1._2.compareTo("0") == 0 || x._1._2.compareTo("2") == 0) {
          false
        } else {
          true
        }
      })
      .map( (y: ((String,String),String)) => (y._1, 1))
      .reduceByKey(_ + _)
      .coalesce(3)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

      eachCodeCount.foreachPartition( x => {

        stockPool.value.getConnect match {

          case Some(connect) => {

            val stockHandle = MysqlHandle(connect)

            testPool.value.getConnect match {

              case Some(conn) => {

                val testHandle = MysqlHandle(conn)

                RddOpt.updateCount(fileInfo, stockHandle, testHandle, x, accum, heatInfo, MixTool.VISIT, stamp, month, day)

                stockHandle.batchExec recover {
                  case e: Exception => {
                    warnLog(fileInfo, "[exec updateCount failure]" + e.getMessage)
                  }
                }

                testHandle.batchExec recover {
                  case e: Exception => {
                    warnLog(fileInfo, "[exec updateTestCount failure]" + e.getMessage)
                  }
                }

                testHandle.close
                stockHandle.close
              }

              case None => warnLog(fileInfo, "Get connect exception")
            }
          }

          case None => warnLog(fileInfo, "Get connect exception")
        }
      })

      if(prevRdd == null) {

        eachCodeCount.map( x => (x._1._1, x._2)).foreachPartition( y => {

          stockPool.value.getConnect match {

            case Some(connect) => {

              val mysqlHandle = MysqlHandle(connect)

              RddOpt.updateAddFirst(mysqlHandle, y, "stock_visit_add", stamp)

              mysqlHandle.batchExec recover {
                case e: Exception => {
                  warnLog(fileInfo, "[exec updateAddFirst failure]" + e.getMessage)
                }
              }

              mysqlHandle.close
            }

            case None => warnLog(fileInfo, "Get connect exception")
          }
        })

      } else {

        eachCodeCount.fullOuterJoin[Int](prevRdd).map( x=> (x._1._1,x._2)).foreachPartition( y => {

          stockPool.value.getConnect match {

            case Some(connect) => {

              val mysqlHandle = MysqlHandle(connect)

              RddOpt.updateAdd(mysqlHandle, y, "stock_visit_add", stamp)

              mysqlHandle.batchExec recover {
                case e: Exception => {
                  warnLog(fileInfo, "[exec updateAdd failure]" + e.getMessage)
                }
              }

              mysqlHandle.close
            }

            case None => warnLog(fileInfo, "Get connect exception")
          }
        })
      }

      prevRdd = eachCodeCount

      eachCodeCount.map( y => {
        (y._1._2, y._2)
      }).reduceByKey(_ + _).foreach( z => {
        RddOpt.updateTotal(fileInfo, stockPool.value.getConnect, MixTool.ALL_VISIT, stamp, z._2)
      })

      RddOpt.updateTime(masterPool.getConnect, "update_visit", stamp)

      accum.setValue(("0", 0))

      if(isAlarm.value == 0) {

        if((isToHour % 12) == 0) {
          TextSender.send("C4545CC1AE91802D2C0FBA7075ADA972", "The real visit data is exception", "15026804656,18600397635")
        }

        isToHour += 1

      }

      isAlarm.setValue(0)

      val stockInfo = heatInfo.value
      val json = MixData("visit", stamp, month, day, hour, stockInfo).toJson.compactPrint

      kafkaProducer.send("0", json)

      heatInfo.setValue(Nil) 
    })

    stc.start
    stc.awaitTermination
    stc.stop()
  }
}
