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
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Created by wukun on 2016/5/23
  * kafka数据操作主程序入口
  */
object SparkKafka {

  def main(args: Array[String]) {
    val xmlHandle = XmlHandle("./config.xml")
    val mysqlPool = MysqlPool(xmlHandle)

    // 这段程序或许以后会用到
    /*val url = xmlHandle.getElem("mySql", "totalurl")
    val sqlHandle = MysqlHandle(url, xmlHandle)

    val stockInfo = sqlHandle.execQuery("select * from SH_SZ_BOARDMAP") recover {
      case e: Exception => {
        println(e.getMessage)
        System.exit(1)
      }
    } */

    /**初始化spark运行上下文环境 */
    val sparkConf = new SparkConf().setAppName("stockHeat")
    val spc = new SparkContext(sparkConf)
    val stc = new StreamingContext(spc, Seconds(300))

    val pool = stc.sparkContext.broadcast(mysqlPool)

    /**初始化kafka参数并创建Dstream对象*/
    val kafkaParam = Map("metadata.broker.list" -> "222.73.34.92:9092")
    val topicParam = "test"
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](stc, kafkaParam, topicParam.split(",").toSet)

    val messages = lines.map( x => {
      x._2
    })

    messages.map( x => MixTool.stockClassify(x)).filter( x => {
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

      averageTime match {
        case Success(z) => {

          pool.value.getConnect match {
            case Some(connect) => {
              val sqlHandle = MysqlHandle(connect)
              sqlHandle.execInsertInto(MixTool.insertSql(z)) recover {
                case e:Exception => System.exit(1)
              }

              sqlHandle.close
            }
            case None => println("not get connect")
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
                    MixTool.insertSql(table, y._1._1, z, y._2)
                    ) recover {
                      case e: Exception => println(e.getMessage)
                    }
                })
                mysqlHandle.close
              }
              case None => println("not get connect")
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
