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
import java.util.Calendar

import com.kunyan.wokongsvc.realtimedata.JsonHandle.StockInfo
import org.apache.spark.Accumulator

import scala.collection.Iterator

/**
  * Created by wukun on 2016/5/19
  * RDD操作接口集合
  */
object RddOpt {

  /**
    * 更新所有股票访问的和
    *
    * @param  rdd   要操作的rdd
    * @param  accum 累加器
    * @param  table 表名
    * @author wukun
    */
  def updateStocksCount(
                         stockHandle: MysqlHandle,
                         rdd: List[((String, Long), Int)],
                         accum: Accumulator[(String, Int)],
                         heatInfo: Accumulator[List[StockInfo]],
                         table: String,
                         currTampStamp: Long) {

    rdd.foreach(y => {

      val stock = y._1._1
      val stockTampMinuteWT = y._1._2
      val number = y._2

      val stockCal: Calendar = Calendar.getInstance
      stockCal.setTimeInMillis(stockTampMinuteWT * 1000)

      val stockMonth = TimeHandle.getMonth(stockCal)
      val stockYear = TimeHandle.getYear(stockCal)
      val stockDay = TimeHandle.getDay(stockCal)

      stockHandle.addCommand(
        MixTool.insertCount(table, stock, stockTampMinuteWT, number)
      ) recover {
        case e: Exception => exception(e)
      }

      stockHandle.addCommand(
        MixTool.insertOldCount(table + "_old", stock, stockTampMinuteWT, number)
      ) recover {
        case e: Exception => exception(e)
      }

      stockHandle.addCommand(
        MixTool.updateAccum(table + "_accum", stock, number)
      ) recover {
        case e: Exception => exception(e)
      }

      stockHandle.addCommand(
        MixTool.updateMonthAccum(table + "_month_", stock, stockMonth, stockDay, number)
      ) recover {
        case e: Exception => exception(e)
      }

      accum +=(stock, number)
      heatInfo += List(StockInfo(stock, number))
    }

    )

  }

  def updateOtherAccum(otherConnect: Option[Connection],
                       table: String): Unit = {

    otherConnect match {

      case Some(otherConnect_) => {

        val otherMysqlHandle = MysqlHandle(otherConnect_)

        otherMysqlHandle.execInsertInto(
          MixTool.deleteData(table)
        ) recover {
          case e: Exception => exception(e)
        }

      }

      case None => logger.warn("Get other connect exception")

    }

  }

  def updateChange(otherConnect: Option[Connection],
                   table: String): Unit = {

    otherConnect match {

      case Some(otherConnect_) => {

        val otherMysqlHandle = MysqlHandle(otherConnect_)

        otherMysqlHandle.execInsertInto(
          MixTool.deleteData(table)
        ) recover {
          case e: Exception => exception(e)
        }

        otherMysqlHandle.execInsertInto(
          MixTool.deleteData(s"${table}_accum")
        ) recover {
          case e: Exception => exception(e)
        }

      }

      case None => logger.warn("Get other connect exception")

    }

  }

  def updateOtherStockCount(stockHandle: MysqlHandle,
                            rdd: List[((String, Long), Int)],
                            table: String,
                            currTampStamp: Long): Unit = {

    rdd.foreach { y =>

      val stock = y._1._1
      val stockTampMinuteWT = y._1._2
      val number = y._2


      stockHandle.addCommand(
        MixTool.insertCount(table + "_old", stock, stockTampMinuteWT, number)
      ) recover {
        case e: Exception => exception(e)
      }

      stockHandle.addCommand(
        MixTool.insertCount(table, stock, stockTampMinuteWT, number)
      ) recover {
        case e: Exception => exception(e)
      }

    }
  }

  def updateChange(mysqlHandle: MysqlHandle,
                   rdd: List[((String, Long), Int)],
                   table: String,
                   currTampStamp: Long): Unit = {

    rdd.foreach { y =>

      val stock = y._1._1
      val stockTampMinuteWT = y._1._2
      val number = y._2

      mysqlHandle.addCommand(
        MixTool.insertCount(table, stock, stockTampMinuteWT, number)
      ) recover {
        case e: Exception => exception(e)
      }

      mysqlHandle.addCommand(
        MixTool.insertOrUpdateAccum(s"${table}_accum", stock, stockTampMinuteWT, number)
      ) recover {
        case e: Exception => exception(e)
      }

    }
  }

  /**
    * 更新所有股票访问的和
    *
    * @param  tryConnect mysql连接
    * @param  table      表名
    * @author wukun
    */
  def updateTotal(tryConnect: Option[Connection],
                  table: String,
                  timeAndCount: (Long, Int)) {

    val stockTamp = timeAndCount._1
    val count = timeAndCount._2

    tryConnect match {

      case Some(connect) => {

        val mysqlHandle = MysqlHandle(connect)

        mysqlHandle.execInsertInto(
          MixTool.insertTotal(table, stockTamp, count)
        ) recover {
          case e: Exception => exception(e)
        }

        mysqlHandle.close()
      }

      case None => logger.warn("Get connect exception")
    }
  }

  //  /**
  //    * 更新所有股票访问的和
  //    *
  //    * @param  rdd   要操作的rdd
  //    * @param  accum 累加器
  //    * @param  table 表名
  //    * @param  currTamp  时间戳
  //    * @author wukun
  //    */
  //  def updateStockCount(
  //                        mysqlHandle: MysqlHandle,
  //                        rdd: Iterator[((String, Long), Int)],
  //                        accum: Accumulator[(String, Int)],
  //                        heatInfo: Accumulator[List[StockInfo]],
  //                        table: String,
  //                        currTamp: Long,
  //                        currMonth: Int,
  //                        currDay: Int) {
  //
  //    rdd.foreach(y => {
  //
  //      mysqlHandle.addCommand(
  //        MixTool.insertCount(table, y._1._1, currTamp, y._2)
  //      ) recover {
  //        case e: Exception => exception(e)
  //      }
  //
  //      mysqlHandle.addCommand(
  //        MixTool.insertOldCount(table + "_old", y._1._1, currTamp, y._2)
  //      ) recover {
  //        case e: Exception => exception(e)
  //      }
  //
  //      mysqlHandle.addCommand(
  //        MixTool.updateAccum(table + "_accum", y._1._1, y._2)
  //      ) recover {
  //        case e: Exception => exception(e)
  //      }
  //
  //
  //      mysqlHandle.addCommand(
  //        MixTool.updateMonthAccum(table + "_month_", y._1._1, currMonth, currDay, y._2)
  //      ) recover {
  //        case e: Exception => exception(e)
  //      }
  //
  //      accum +=(y._1._1, y._2)
  //      heatInfo += List(StockInfo(y._1._1, y._2))
  //    })
  //
  //  }

  //  /**
  //    * 更新所有股票访问的和
  //    *
  //    * @param  fileInfo 日志所需的文件信息
  //    * @param  rdd      要操作的rdd
  //    * @param  table    表名
  //    * @param  tamp     时间戳
  //    * @author wukun
  //    */
  //  def updateStockCount(
  //                        fileInfo: (String, String),
  //                        mysqlHandle: MysqlHandle,
  //                        rdd: Iterator[((String, String), Int)],
  //                        table: String,
  //                        tamp: Long,
  //                        month: Int,
  //                        day: Int) {
  //
  //    rdd.foreach(y => {
  //
  //      mysqlHandle.addCommand(
  //        MixTool.insertOldCount(table + "_old", y._1._1, tamp, y._2)
  //      ) recover {
  //        case e: Exception => exception(e)
  //      }
  //
  //      mysqlHandle.addCommand(
  //        MixTool.updateMonthAccum(table + "_month_", y._1._1, month, day, y._2)
  //      ) recover {
  //        case e: Exception => exception(e)
  //      }
  //
  //    })
  //  }

  /**
    * 更新所有股票访问的和
    *
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
          case e: Exception => exception(e)
        }

        mysqlHandle.close
      }

      case None => logger.error("get connection failure")
    }
  }

  /**
    * 更新所有股票访问的和
    *
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
          case e: Exception => exception(e)
        }

        mysqlHandle.close
      }

      case None => logger.error("get connection failure")
    }
  }

  /**
    * 更新所有股票访问的和
    *
    * @param  table    表名
    * @param  currTamp 时间戳
    * @author wukun
    */
  def updateAddFirst(
                      mysqlHandle: MysqlHandle,
                      rdd: Iterator[((String, Long), Int)],
                      table: String,
                      currTamp: Long) {

    rdd.foreach(y => {

      val stock = y._1._1

      mysqlHandle.addCommand(
        MixTool.insertCount(table, stock, currTamp, y._2)
      ) recover {
        case e: Exception => exception(e)
      }

    })
  }

  /**
    * 更新所有股票访问的和
    *
    * @param  rdd         要操作的rdd
    * @param  table       表名
    * @param  currentTamp 时间戳
    * @author wukun
    *         这里面比较可能有问题，但是好像改数据库并没有使用，因此此处并没有更新
    */
  def updateAdd(
                 mysqlHandle: MysqlHandle,
                 rdd: Iterator[((String, Long), (Option[Int], Option[Int]))],
                 table: String,
                 currentTamp: Long) {

    rdd.foreach(y => {

      val now = y._2._1 match {
        case Some(z) => z
        case None => 0
      }

      val prev = y._2._2 match {
        case Some(z) => z
        case None => 0
      }

      mysqlHandle.addCommand(
        MixTool.insertCount(table, y._1._1, currentTamp, now - prev)
      ) recover {
        case e: Exception => exception(e)
      }

    })
  }

  /**
    * 更新所有股票访问的和
    *
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
          case e: Exception => exception(e)
        }

        mysqlHandle.execInsertInto(
          MixTool.deleteData(table)
        ) recover {
          case e: Exception => exception(e)
        }

        mysqlHandle.execInsertInto(
          MixTool.deleteData(table + "_add")
        ) recover {
          case e: Exception => exception(e)
        }

        mysqlHandle.execInsertInto(
          MixTool.deleteData(table + "_count")
        ) recover {
          case e: Exception => exception(e)
        }
        mysqlHandle.execInsertInto(
          MixTool.deleteTime(table)
        ) recover {
          case e: Exception => exception(e)
        }

        mysqlHandle.close
      }

      case None => logger.error("[Get connect failure]")
    }


  }

}
