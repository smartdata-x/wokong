/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/SparkFile.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-06-01 20:51
#    Description  : 
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
  * Created by wukun on 2016/06/01
  * 静态文件操作主入口
  */
object SparkSearch extends CustomLogger {

  def main(args: Array[String]) {

    if(args.length != 2) {
      errorLog(fileInfo, "args too little")
      System.exit(-1)
    }

   PropertyConfigurator.configure(args(0))

   val xmlHandle = XmlHandle(args(1))

   val execPool = MysqlPool(xmlHandle)
   val masterPool = MysqlPool(xmlHandle)
   masterPool.setConfig(1, 1, 3)

   val kafkaProducer = KafkaProducer(xmlHandle)

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

    val sparkConf = new SparkConf().setAppName("searchHeat")
    val spc = new SparkContext(sparkConf)
    val stc = new StreamingContext(spc, Seconds(60))

    var prevRdd: RDD[((String, String), Int)] = null

    val stockPool = stc.sparkContext.broadcast(execPool)

    val alias = stc.sparkContext.broadcast(stockalias.asInstanceOf[Tuple2Map])

    /* 初始化计算最大股票访问量；累加器 */
    val accum = stc.sparkContext.accumulator[(String, Int)](("0", 0))
    val heatInfo = stc.sparkContext.accumulator[List[StockInfo]](Nil)
    var isToHour: Long = 0L
    var lastUpdateTime = 0

    /* 初始化kafka参数并创建Dstream对象 */
    val kafkaParam = Map("metadata.broker.list" -> xmlHandle.getElem("kafka", "broker"), "group.id" -> "search")
    val topicParam = xmlHandle.getElem("kafka", "topic")
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](stc, kafkaParam, topicParam.split(",").toSet)
    .foreach( rows => {

      val cal: Calendar = Calendar.getInstance
      val nowUpdateTime = TimeHandle.getDay
      val hour = TimeHandle.getNowHour(cal)
      if(nowUpdateTime != lastUpdateTime && hour == 0) {
        RddOpt.updateAccum(masterPool.getConnect, "stock_search", 0)
        lastUpdateTime = nowUpdateTime
      }

      val month = TimeHandle.getMonth(cal, 1)
      val day = TimeHandle.getDay(cal)
      val stamp = TimeHandle.getStamp(cal)

      val eachCodeCount = rows
      .map( row => row._2)
      .map( x => MixTool.stockClassify(x, alias.value))
      .filter( x => {

        if(x._1._2.compareTo("0") == 0 || x._1._2.compareTo("1") == 0) {
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

            RddOpt.updateCount(fileInfo, stockHandle, x, accum, heatInfo, MixTool.SEARCH, stamp, month, day)

            stockHandle.batchExec recover {
              case e: Exception => {
                warnLog(fileInfo, "[exec updateCount failure]" + e.getMessage)
              }
            }

            stockHandle.close
          }

          case None => warnLog(fileInfo, "Get connect exception")
        }
      })

      if(prevRdd == null) {

        eachCodeCount.map( x => (x._1._1, x._2)).foreachPartition( y => {

          stockPool.value.getConnect match {

            case Some(connect) => {

              val mysqlHandle = MysqlHandle(connect)

              RddOpt.updateAddFirst(mysqlHandle, y, "stock_search_add", stamp)

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

              RddOpt.updateAdd(mysqlHandle, y, "stock_search_add", stamp)

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
        RddOpt.updateTotal(fileInfo, stockPool.value.getConnect, MixTool.ALL_SEARCH, stamp, z._2)
      })

      RddOpt.updateTime(masterPool.getConnect, "update_search", stamp)

      accum.setValue(("0", 0))

      val stockInfo = heatInfo.value
      val json = MixData("search", stamp, month, day, hour, stockInfo).toJson.compactPrint

      val topic = xmlHandle.getElem("kafkaconsumer", "searchtopic")

      kafkaProducer.send(topic, "0", json)

      heatInfo.setValue(Nil) 
    })

    stc.start
    stc.awaitTermination
    stc.stop()
  }
}

