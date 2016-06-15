/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/RddOpt.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-06-02 11:37
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import java.sql.Connection
import scala.Option
import scala.Some
import scala.None
import org.apache.spark.rdd.RDD
import org.apache.spark.Accumulator
import scala.collection.Iterator

/**
  * Created by wukun on 2016/5/19
  * RDD操作接口集合
  */
object RddOpt extends CustomLogger {

  /**
    * 更新所有股票访问的和
    * @param  fileInfo   日志所需的文件信息
    * @param  tryConnect mysql连接
    * @param  table      表名
    * @param  tamp       时间戳
    * @param  count      访问次数
    * @author wukun
    */
  def updateTotal(
    fileInfo: (String, String),
    tryConnect: Option[Connection],
    table: String, 
    tamp: Long, 
    count: Int) {

      tryConnect match {
        case Some(connect) => {

          val mysqlHandle = MysqlHandle(connect)

          mysqlHandle.execInsertInto(
            MixTool.insertTotal(table, tamp, count)
          ) recover {
            case e: Exception => warnLog(fileInfo, e.getMessage + "[Update data_sum failure]")
          }

          mysqlHandle.close
        }

        case None => warnLog(fileInfo, "Get connect exception")
      }
  }

  /**
    * 更新所有股票访问的和
    * @param  fileInfo   日志所需的文件信息
    * @param  tryConnect mysql连接
    * @param  rdd        要操作的rdd
    * @param  accum      累加器
    * @param  table      表名
    * @param  tamp       时间戳
    * @author wukun
    */
  def updateCount (
    fileInfo: (String, String),
    tryConnect: Option[Connection],
    rdd: Iterator[((String, String), Int)],
    accum: Accumulator[(String, Int)],
    table: String,
    tamp: Long) {
      tryConnect match {
        case Some(connect) => {
          val mysqlHandle = MysqlHandle(connect)

          rdd.foreach( y => {

            mysqlHandle.execInsertInto(
              MixTool.insertCount(table, y._1._1, tamp, y._2)
            ) recover {
              case e: Exception => warnLog(fileInfo, e.getMessage)
            }

            mysqlHandle.execInsertInto(
              MixTool.insertOldCount(table + "_old", y._1._1, tamp, y._2)
            ) recover {
              case e: Exception => warnLog(fileInfo, e.getMessage)
            }

            mysqlHandle.execInsertInto(
              MixTool.updateAccum(table + "_accum", y._1._1, y._2)
            ) recover {
              case e: Exception => warnLog(fileInfo, e.getMessage)
            }

            accum += (y._1._1, y._2)
          })

          mysqlHandle.close
        }

        case None => warnLog(fileInfo, "Get connect exception")
      }
  }

  /**
    * 更新所有股票访问的和
    * @param  tryConnect mysql连接
    * @param  table      表名
    * @param  tamp       时间戳
    * @author wukun
    */
  def updateTime(
    tryConnect: Option[Connection],
    table: String,
    tamp: Long) {
      tryConnect match {
        case Some(connect) => {
          val mysqlHandle = MysqlHandle(connect)

          mysqlHandle.execInsertInto(MixTool.insertTime(table, tamp)) recover {
            case e:Exception => warnLog(fileInfo, e.getMessage + "[Update time failure]")
          }

          mysqlHandle.close
        }

        case None => warnLog(fileInfo, "Get connect exception")
      }
  }

  /**
    * 更新所有股票访问的和
    * @param  tryConnect mysql连接
    * @param  table      表名
    * @param  recode     字段
    * @param  max        要更新的最大值
    * @author wukun
    */
  def updateMax(
    tryConnect: Option[Connection],
    table: String,
    recode: String,
    max: Int) {
      tryConnect match {
        case Some(connect) => {
          val mysqlHandle = MysqlHandle(connect)

          mysqlHandle.execUpdate(MixTool.updateMax(table, recode, max)) recover {
            case e:Exception => warnLog(fileInfo, e.getMessage + "[Update time failure]")
          }

          mysqlHandle.close
        }

        case None => warnLog(fileInfo, "Get connect exception")
      }
  }

  /**
    * 更新所有股票访问的和
    * @param  tryConnect mysql连接
    * @param  rdd        要操作的rdd
    * @param  table      表名
    * @param  tamp       时间戳
    * @author wukun
    */
  def updateAddFirst(
    tryConnect: Option[Connection], 
    rdd: Iterator[(String, Int)],
    table: String, 
    tamp: Long) {

      tryConnect match {
        case Some(connect) => {

          val mysqlHandle = MysqlHandle(connect)

          rdd.foreach( y => {

            mysqlHandle.execInsertInto(
              MixTool.insertCount(table, y._1, tamp, y._2)
            ) recover {
              case e: Exception => warnLog(fileInfo, e.getMessage + "[Update data failure]")
            }
          })
          mysqlHandle.close
        }

        case None => warnLog(fileInfo, "[Get connect failure]")
      }
  }

  /**
    * 更新所有股票访问的和
    * @param  tryConnect mysql连接
    * @param  rdd        要操作的rdd
    * @param  table      表名
    * @param  tamp       时间戳
    * @author wukun
    */
  def updateAdd(
    tryConnect: Option[Connection], 
    rdd: Iterator[(String, (Option[Int], Option[Int]))],
    table: String, 
    tamp: Long) {

      tryConnect match {
        case Some(connect) => {

          val mysqlHandle = MysqlHandle(connect)

          rdd.foreach( y => {

            val now = y._2._1 match {
              case Some(z) => z
              case None    => 0
            }

            val prev = y._2._2 match {
              case Some(z) => z
              case None    => 0
            }

            mysqlHandle.execInsertInto(
              MixTool.insertCount(table, y._1, tamp, now - prev)
            ) recover {
              case e: Exception => warnLog(fileInfo, e.getMessage + "[Update data failure]")
            }
          })
          mysqlHandle.close
        }

        case None => warnLog(fileInfo, "[Get connect failure]")
      }
  }

  /**
    * 更新所有股票访问的和
    * @param  tryConnect mysql连接
    * @param  accum      累加器
    * @param  table      表名
    * @author wukun
    */
  def updateAccum(
    tryConnect: Option[Connection],
    table: String,
    accum: Int) {

      tryConnect match {
        case Some(connect) => {
          val mysqlHandle = MysqlHandle(connect)

          mysqlHandle.execInsertInto(
            MixTool.updateAccum(table + "_accum", accum)
          ) recover {
            case e: Exception => warnLog(fileInfo, e.getMessage + "[Update accum time failure]")
          }

          mysqlHandle.execInsertInto(
            MixTool.deleteCount(table)
          ) recover {
            case e: Exception => warnLog(fileInfo, e.getMessage + "[delete count failure]")
          }

          mysqlHandle.close
        }

         case None => warnLog(fileInfo, "[Get connect failure]")
      }
  }
}
