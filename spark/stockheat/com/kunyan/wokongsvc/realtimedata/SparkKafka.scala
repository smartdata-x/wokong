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

import com.kunyan.wokongsvc.realtimedata.MixTool.{Tuple2Map, TupleHashMapSet}
import CustomAccum._

import kafka.serializer.StringDecoder
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.rdd.RDD 
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

import java.util.Calendar
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer

/**
  * Created by wukun on 2016/5/23
  * kafka数据操作主程序入口
  */
object SparkKafka extends CustomLogger {

  def main(args: Array[String]) {

    if(args.length != 2) {

      errorLog(fileInfo, "args too little")
      System.exit(-1)

    }

    /* 加载日志配置文件 */
    PropertyConfigurator.configure(args(0))
    /* 初始化应用程序配置文件 */
    val xmlHandle = XmlHandle(args(1))
    /* 初始化执行节点mysql连接池 */
    val execPool = MysqlPool(xmlHandle)
    /* 初始化驱动节点mysql连接池 */
    val masterPool = MysqlPool(xmlHandle)
    masterPool.setConfig(1, 2)
    /* 初始化股票名称的各种表示方式 */
    val stockInfo = masterPool.getConnect match {

      case Some(connect) => {

        val sqlHandle = MysqlHandle(connect)

        val alias = sqlHandle.execQueryStockAlias(MixTool.SYN_SQL) match {

          case Success(z) => z
          case Failure(e) => {
            errorLog(fileInfo, e.getMessage + "[Query stockAlias exception]")
            System.exit(-1)
          }

        }

        val stockHyGn = sqlHandle.execQueryHyGn(MixTool.STOCK_HY_GN) recover {
          case e: Exception => {
            errorLog(fileInfo, e.getMessage + "[initial stock_hy_gn exception]")
            System.exit(-1)
          }
        }

        sqlHandle.close
        (alias, stockHyGn)
      }

      case None => {
        errorLog(fileInfo, "[Get mysql connect failure]")
        System.exit(-1)
      }
    }

    val molt = stockInfo.asInstanceOf[(Tuple2Map, Try[TupleHashMapSet])]
    val stockAlias = molt._1
    val hyGn = molt._2.get._2
    val stockHyGn = molt._2.get._1

    /* 初始化spark运行上下文环境 */
    val sparkConf = new SparkConf().setAppName("visitHeat")
    val spc = new SparkContext(sparkConf)
    val stc = new StreamingContext(spc, Seconds(300))

    /* 广播执行节点mysql连接池 */
    val pool = stc.sparkContext.broadcast(execPool)
    /* 广播股票的各种别名 */
    val alias = stc.sparkContext.broadcast(stockAlias)
    /* 广播行业与股票的映射 */
    val stockHy = stc.sparkContext.broadcast[HashMap[String, ListBuffer[String]]](stockHyGn._1)
    /* 广播概念与股票的映射 */
    val stockGn = stc.sparkContext.broadcast(stockHyGn._2)

    /* 初始化计算某时刻A股访问总量；累加器 */
    val accumATotal = stc.sparkContext.accumulator[Int](0)
    var lastUpdateTime = 0

    var prevRdd: RDD[(String, Int)] = null
    var prevHyGn: HashMap[String, Int] = null
    var temp: HashMap[String, Int] = new HashMap[String, Int]

    /* 初始化kafka参数并创建Dstream对象 */
    val kafkaParam = Map("metadata.broker.list" -> xmlHandle.getElem("kafka", "broker"))
    val topicParam = xmlHandle.getElem("kafka", "topic")
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](stc, kafkaParam, topicParam.split(",").toSet)

    /* 进入工作逻辑 */
    val messages = lines.map( x => {
      x._2
    })

    /* 0是异常的股票代码，2是搜索种类的股票代码 */
    messages.map( x => MixTool.stockClassify(x, alias.value)).filter( x => {

      if(x._1._2.compareTo("0") == 0 || x._1._2.compareTo("2") == 0) {
        false
      } else {
        true
      }

    }).foreach( x => {

      /* 每天0时清空不保存历史数据的表 */
      val cal: Calendar = Calendar.getInstance
      val month = TimeHandle.getMonth(cal, 1)
      val day = TimeHandle.getDay(cal)

      if(day != lastUpdateTime && TimeHandle.getNowHour(cal) == 0) {

        RddOpt.resetData(masterPool.getConnect, "proc_resetData")
        lastUpdateTime = day

      } else {
        /* 每次更新都要清空diff表，这个表不需要保存一整天的 */
        RddOpt.resetDiffData(masterPool.getConnect, "proc_resetDiffData")
      }

      /* 计算这个月的数据存在哪个月份表里 */
      val monthTable = month%2 match {
        case 0 => ("s_v_month_prev", "hy_v_month_prev", "gn_v_month_prev")
        case 1 => ("s_v_month_now", "hy_v_month_now", "gn_v_month_now")
      }

      /* x._1._1是股票代码，x._1._2是股票种类，x._2是访问时间戳 */
      val timeTamp = x.map((y: ((String, String), String)) => {
        new StringBuilder(y._2).toLong
      }).persist(StorageLevel.MEMORY_AND_DISK_SER)

      val count = timeTamp.count
      val sum = timeTamp.fold(0)((x,y) => x + y )

      /* 计算平均时间戳, 此处是微秒为单位 */
      val averageTime = {
        Try(sum / count)
      }

      averageTime match {

        case Success(z) => {

          val timeTamp = z / 1000

          /* 计算相同种类下的相同股票访问次数 */
          val eachCodeCount = x.map( (y: ((String,String),String)) => {
            (y._1._1, 1)
          }).reduceByKey(_ + _).persist(StorageLevel.MEMORY_AND_DISK_SER)

          if(prevRdd == null) {

            eachCodeCount.map( x => (x._1, x._2)).foreachPartition( y => {
              RddOpt.updateADataFirst(pool.value.getConnect, y, "proc_visit_A_data", timeTamp, monthTable._1, day, accumATotal)
            })

          } else {

            eachCodeCount.fullOuterJoin[Int](prevRdd).map( x=> (x._1, x._2)).foreachPartition( y => {
              RddOpt.updateAData(pool.value.getConnect, y, "proc_visit_A_data", timeTamp, monthTable._1, "s_v_diff", day, accumATotal)
            })

          }

          prevRdd = eachCodeCount

          val nowHyGnData = eachCodeCount.flatMap( x => {

            /* gnInfo保存的是单只股票对应的所有概念 */
            val gnInfo = stockGn.value.getOrElse(x._1, new ListBuffer[String])

            /* hyInfo保存的是单只股票对应的所有行业 */
            val hyInfo = stockHy.value.getOrElse(x._1, new ListBuffer[String])

            hyInfo.map( y => (y, x._2)) ++ gnInfo.map( z => (z, x._2))
          }).reduceByKey(_ + _).collectAsMap

          nowHyGnData.foreach( x => {

            /* kind为0代表行业 ,1代表概念 */
            var kind = 0
            var table: String = monthTable._2

            if(hyGn._2(x._1)) {

              kind = 1
              table = monthTable._3

            }

            temp += (x)
            
            val diff = {

              if(prevHyGn != null) {

                prevHyGn.get(x._1) match {

                  case Some(v) => {
                    prevHyGn -= x._1
                    x._2 - v
                  }
                  case None => {
                    x._2
                  }

                }

              } else {
                x._2
              }
            }

            RddOpt.updateHyGnData(masterPool.getConnect, x, diff, "proc_visit_hygn_data", timeTamp, day, kind, table)
          })

          if(prevHyGn != null) {

            prevHyGn.foreach( x => {

              if(hyGn._1(x._1)) {
                RddOpt.updateHyGnDiff(masterPool.getConnect, x._1, timeTamp, "hy_v_diff", 0 - x._2)
              } else {
                RddOpt.updateHyGnDiff(masterPool.getConnect, x._1, timeTamp, "gn_v_diff", 0 - x._2)
              }
            })

          }

          /* 更新单个时间总和与更新时间 */
          RddOpt.updateTotalTime(masterPool.getConnect, timeTamp, accumATotal.value)

          prevHyGn = temp
          temp.clear
        }

        case Failure(e) => e
      }

      accumATotal.setValue(0)
    })


    stc.start
    stc.awaitTermination
    stc.stop()
  }
}
