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
import MixTool.Tuple3Map

/**
  * Created by wukun on 2016/5/23
  * kafka数据操作主程序入口
  */
object SparkKafka extends CustomLogger {


  def main(args: Array[String]) {

    if(args.length != 3) {
      errorLog(fileInfo, "args too little")
      System.exit(-1)
    }

    /** 加载日志配置文件 */
    PropertyConfigurator.configure(args(1))

    val xmlHandle = XmlHandle(args(2))
    /** 初始化mysql连接池 */
    val mysqlPool = MysqlPool(xmlHandle)

    val url = xmlHandle.getElem("mySql", "totalurl")
    val sqlHandle = MysqlHandle(url, xmlHandle)

    /** 初始化股票名称的各种表示方式 */
    val stockalias = sqlHandle.execQueryStockAlias(MixTool.SQL) recover {
      case e: Exception => {
        errorLog(fileInfo, e.getMessage + "[Query stockAlias exception]")
        System.exit(-1)
      }
    }

    /**初始化spark运行上下文环境 */
    val sparkConf = new SparkConf().setAppName("stockHeat")
    val spc = new SparkContext(sparkConf)
    val stc = new StreamingContext(spc, Seconds(300))

    /** 广播连接池和股票的各种名称 */
    val pool = stc.sparkContext.broadcast(mysqlPool)
    val alias = stc.sparkContext.broadcast(stockalias.asInstanceOf[Tuple3Map])

    /**初始化kafka参数并创建Dstream对象*/
    val kafkaParam = Map("metadata.broker.list" -> "222.73.34.92:9092")
    val topicParam = "test"
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](stc, kafkaParam, topicParam.split(",").toSet)

    val messages = lines.map( x => {
      x._2
    })

    messages.map( x => MixTool.stockClassify(x, alias.value)).filter( x => {
      if(x._1._2.compareTo("0") == 0) {
        false
      } else {
        true
      }
    }).foreach( x => {

      val timeTamp = x.map( (y: ((String, String), String)) => {
        new StringBuilder(y._2).toLong
      }).persist(StorageLevel.MEMORY_AND_DISK_SER)

      val count = timeTamp.count
      val sum = timeTamp.fold(0)((x,y) => x + y )

      val averageTime = {
        Try(sum / count)
      }

      warnLog(fileInfo, "get")
      averageTime match {
        case Success(z) => {

          val timeTamp = z / 1000

          pool.value.getConnect match {
            case Some(connect) => {
              val sqlHandle = MysqlHandle(connect)
              sqlHandle.execInsertInto(MixTool.insertSql(timeTamp)) recover {
                case e:Exception => warnLog(fileInfo, e.getMessage + "[Update time failure]")
              }

              sqlHandle.close
            }
            case None => warnLog(fileInfo, "Get connect exception")
          }

          x.map( (y:((String,String),String)) => {
            (y._1, 1)
          }).reduceByKey(_ + _).foreachPartition( x => {

            pool.value.getConnect match {
              case Some(connect) => {

                val mysqlHandle = MysqlHandle(connect)

                x.foreach( y => {
                  val table = y._1._2 match {
                    case "1" => MixTool.VISIT 
                    case "2" => MixTool.SEARCH
                  }

                  mysqlHandle.execInsertInto(
                    MixTool.insertSql(table, y._1._1, timeTamp, y._2)
                    ) recover {
                      case e: Exception => warnLog(fileInfo, e.getMessage + "[Update data failure]")
                    }
                })
                mysqlHandle.close
              }
              case None => warnLog(fileInfo, "Get connect exception")
            }
          })
        }
        case Failure(e) => e
      }
    })

    stc.start
    stc.awaitTermination
    stc.stop()
  }
}
