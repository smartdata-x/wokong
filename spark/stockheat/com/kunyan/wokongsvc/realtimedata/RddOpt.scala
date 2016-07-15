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
    tamp: Long,
    month: Int,
    day: Int) {
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

            mysqlHandle.execInsertInto(
              MixTool.updateMonthAccum(table + "_month_", y._1._1, month, day, y._2)
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
  def updateTotalTime(
    tryConnect: Option[Connection],
    tamp: Long,
    total: Long) {

      tryConnect match {

        case Some(connect) => {
          val mysqlHandle = MysqlHandle(connect)

          mysqlHandle.execTotalTimeProc(
            "{call proc_updateTotalAndTime(?, ?)}", tamp, total
          ) recover {
            case e:Exception => warnLog(fileInfo, e.getMessage + "[Update total and time failure]")
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
  def updateADataFirst(
    tryConnect: Option[Connection], 
    rdd: Iterator[(String, Int)],
    proc: String,
    tamp: Long,
    monthTable: String,
    day: Int,
    accumulator: Accumulator[Int]) {

      tryConnect match {
        case Some(connect) => {

          val mysqlHandle = MysqlHandle(connect)

          rdd.foreach( x => {

            accumulator += x._2

            mysqlHandle.execADataProc(
              "{call " + proc + "(?, ?, ?, ?, ?, ?)}", x._1, tamp, x._2, x._2 , monthTable, day
            ) recover {
              case e: Exception => warnLog(fileInfo, e.getMessage + "[Update visit A data failure]")
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
  def updateAData(
    tryConnect: Option[Connection], 
    rdd: Iterator[(String, (Option[Int], Option[Int]))],
    proc: String,
    tamp: Long,
    monthTable: String,
    diffTable: String, 
    day: Int,
    accumulator: Accumulator[Int]) {

      tryConnect match {
        case Some(connect) => {

          val mysqlHandle = MysqlHandle(connect)

          rdd.foreach( x => {

            val now = x._2._1 match {
              case Some(z) => {
                accumulator += z
                z
              }
              case None    => 0
            }

            val prev = x._2._2 match {
              case Some(z) => z
              case None    => 0
            }

            if(now != 0) {

              mysqlHandle.execADataProc(
                "{call " + proc + "(?, ?, ?, ?, ?, ?)}", x._1, tamp, now, now - prev, monthTable, day
              ) recover {
                case e: Exception => warnLog(fileInfo, e.getMessage + "[Update visit A data failure]")
              }


            } else {

              mysqlHandle.execInsertInto(
                MixTool.insertCount(diffTable, x._1, tamp, now - prev)
              ) recover {
                case e: Exception => warnLog(fileInfo, e.getMessage + "[Update diff data failure]")
              }

            }
          })

          mysqlHandle.close
        }

        case None => warnLog(fileInfo, "[Get connect failure]")
      }
  }

  /**
    * 更新行业和概念数据
    * @param  tryConnect mysql连接
    * @param  data 统计的数据
    * @param  diff 差值
    * @param  proc 存储过程名称
    * @param  tamp 时间戳
    * @param  day  第几天
    * @param  distinct 月份标识
    * @param  table 表名
    * @author wukun
    */
  def updateHyGnData(
    tryConnect: Option[Connection], 
    data: (String, Int),
    diff: Int,
    proc: String,
    tamp: Long,
    day: Int,
    distinct: Int,
    table: String) {

      tryConnect match {
        case Some(connect) => {

          val mysqlHandle = MysqlHandle(connect)

          mysqlHandle.execHyGnDataProc(
            "{call " + proc + "(?, ?, ?, ?, ?, ?, ?)}", data._1, tamp, data._2, diff, table, day, distinct
          ) recover {
            case e: Exception => warnLog(fileInfo, e.getMessage + "[Update visit HYGN data failure]")
          }

          mysqlHandle.close
        }

        case None => warnLog(fileInfo, "[Get connect failure]")
      }
  }

  /**
    * 更新行业和概念差值
    * @param  tryConnect mysql连接
    * @param  name 行业或概念名称
    * @param  diff 差值
    * @param  tamp 时间戳
    * @param  table 表名
    * @author wukun
    */
  def updateHyGnDiff(
    tryConnect: Option[Connection],
    name: String,
    tamp: Long,
    table: String,
    diff: Int) {

      tryConnect match {

        case Some(connect) => {

          val mysqlHandle = MysqlHandle(connect)

          mysqlHandle.execHyGnDiff(
            "insert into " + table + " values('" + name + "'," + tamp + "," + diff + ")"
          ) recover {
            case e: Exception => warnLog(fileInfo, e.getMessage + "[Update visit HYGN diff data failure]")
          }

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

  /**
    * 重置差值数据
    * @param  tryConnect mysql连接
    * @param  proc       存储过程名称
    * @author wukun
    */
  def resetDiffData(
    tryConnect: Option[Connection],
    proc: String) {

      tryConnect match {

        case Some(connect) => {

          val mysqlHandle = MysqlHandle(connect)

          mysqlHandle.execDiffInit(
            "{call " + proc + "()}"
          ) recover {
            case e: Exception => warnLog(fileInfo, e.getMessage + "[delete diff failure]")
          }

          mysqlHandle.close
        }

         case None => warnLog(fileInfo, "[Get connect failure]")
      }
  }

  /**
    * 重置数据
    * @param  tryConnect mysql连接
    * @param  proc       存储过程名称
    * @author wukun
    */
  def resetData(
    tryConnect: Option[Connection],
    proc: String) {

    tryConnect match {

      case Some(connect) => {

        val mysqlHandle = MysqlHandle(connect)

        mysqlHandle.execInitProc(
          "{call "+ proc + "()}"
        ) recover {
          case e: Exception => warnLog(fileInfo, e.getMessage + "[delete A data failure]")
        }

        mysqlHandle.close
      }

      case None => warnLog(fileInfo, "[Get connect failure]")
    }
  }
}
