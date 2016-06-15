/*============================================================================= 
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/spark_kafka/src/main/scala/SparkKafka.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-05-17 21:34
#    Description  : 
=============================================================================*/
package com.kunyan.wokongsvc.realtimedata

import kafka.serializer.StringDecoder
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import MixTool.Tuple2Map
import org.apache.spark.rdd.RDD 
import CustomAccum._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

/**
  * Created by wukun on 2016/5/23
  * kafka数据操作主程序入口
  */
object SparkKafka extends CustomLogger {

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

    /* 初始化mysql连接池 */
    val masterPool = MysqlPool(xmlHandle)
    masterPool.setConfig(1, 2)

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

    /* 初始化股票和行业及概念的映射 */
    /*val stockHyGn = sqlHandle.execQueryHyGn(MixTool.SYN_SQL) recover {
      case e: Exception => {
        errorLog(fileInfo, e.getMessage + "[initial stock_hy_gn exception]")
        System.exit(-1)
      }
    }

    sqlHandle.close */

    /* 初始化spark运行上下文环境 */
    val sparkConf = new SparkConf().setAppName("visitHeat")
    val spc = new SparkContext(sparkConf)
    val stc = new StreamingContext(spc, Seconds(300))

    var prevRdd: RDD[((String, String), Int)] = null

    /* 广播连接池、股票和到行业及概念的映射 */
    val pool = stc.sparkContext.broadcast(execPool)
    val alias = stc.sparkContext.broadcast(stockalias.asInstanceOf[Tuple2Map])
    /*val stockHy = stc.sparkContext.broadcast(stockHyGn.get.asInstanceOf[TupleHashMap]._1)
    val stockGn = stc.sparkContext.broadcast(stockHyGn.get.asInstanceOf[TupleHashMap]._2) */

    /* 初始化计算最大股票访问量的；累加器 */
    val accum = stc.sparkContext.accumulator[(String, Int)](("0", 0))
    var lastUpdateTime = 0


    /* 初始化kafka参数并创建Dstream对象 */
    val kafkaParam = Map("metadata.broker.list" -> xmlHandle.getElem("kafka", "broker"))
    val topicParam = xmlHandle.getElem("kafka", "topic")
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](stc, kafkaParam, topicParam.split(",").toSet)

    val messages = lines.map( x => {
      x._2
    })

    messages.map( x => MixTool.stockClassify(x, alias.value)).filter( x => {
      if(x._1._2.compareTo("0") == 0 || x._1._2.compareTo("2") == 0) {
        false
      } else {
        true
      }
    }).foreach( x => {

      val nowUpdateTime = TimeHandle.getDay
      if(nowUpdateTime != lastUpdateTime && TimeHandle.getNowHour == 0) {
        RddOpt.updateAccum(masterPool.getConnect, "stock_visit", 0)
        lastUpdateTime = nowUpdateTime
      }

      val timeTamp = x.map( (y: ((String, String), String)) => {
        new StringBuilder(y._2).toLong
      }).persist(StorageLevel.MEMORY_AND_DISK_SER)

      val count = timeTamp.count
      val sum = timeTamp.fold(0)((x,y) => x + y )

      val averageTime = {
        Try(sum / count)
      }

      averageTime match {
        case Success(z) => {

          val timeTamp = z / 1000

          val eachCodeCount = x.map( (y: ((String,String),String)) => {
            (y._1, 1)
          }).reduceByKey(_ + _).persist(StorageLevel.MEMORY_AND_DISK_SER)

          eachCodeCount.foreachPartition( x => {

            RddOpt.updateCount(fileInfo, pool.value.getConnect, x, accum, MixTool.VISIT, timeTamp)
          })

          /*eachCodeCount.flatMap( x => {

            val hyInfo = stockHy.value(x._1._1)
            val gnInfo = stockGn.value(x._1._1)

            hyInfo.map( y => ((0, y), x._2)) ++ gnInfo.map( z => ((0, z), x._2))
          }).reduceByKey(_ + _).foreach(println(_)) */

          RddOpt.updateMax(masterPool.getConnect, "stock_max", "max_v", accum.value._2)

          if(prevRdd == null) {
            eachCodeCount.map( x => (x._1._1, x._2)).foreachPartition( y => {
              RddOpt.updateAddFirst(pool.value.getConnect, y, "stock_visit_add", timeTamp)
            })
          } else {
            eachCodeCount.fullOuterJoin[Int](prevRdd).map( x=> (x._1._1,x._2))foreachPartition( y => {
              RddOpt.updateAdd(pool.value.getConnect, y, "stock_visit_add", timeTamp)
            })
          }

          prevRdd = eachCodeCount

          eachCodeCount.map( y => {
            (y._1._2, y._2)
          }).reduceByKey(_ + _).foreach( z => {
            RddOpt.updateTotal(fileInfo, pool.value.getConnect, MixTool.ALL_VISIT, timeTamp, z._2)
          })

          RddOpt.updateTime(masterPool.getConnect, "update_visit", timeTamp)
        }

        case Failure(e) => e
      }

      accum.setValue(("0", 0))
    })


    stc.start
    stc.awaitTermination
    stc.stop()
  }
}
