/*=============================================================================
  # Copyright (c) 2015
  # ShanghaiKunyan.  All rights reserved
  #
  # Filename : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/TimerHandle.scala
  # Author   : Sunsolo
  # Email    : wukun@kunyan-inc.com
  # Date     : 2016-05-22 16:04
  =============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import java.io.FileWriter
import java.util.{Calendar, Timer, TimerTask}

import com.kunyan.wokongsvc.realtimedata.CustomAccum._
import com.kunyan.wokongsvc.realtimedata.logger.HeatLogger
import org.apache.hadoop.hbase.TableNotFoundException
import org.apache.hadoop.hbase.client.RetriesExhaustedException
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * Created by wukun on 2016/5/23
  * 定时任务实现类, 每隔5分钟提交一次作业
  */
class TimerHandle(
                   hbaseContext: HbaseContext,
                   pool: MysqlPool,
                   stock: mutable.HashSet[String],
                   path: String,
                   tamp: Long
                 ) extends TimerTask with Serializable {

  @transient val hc = hbaseContext
  @transient val masterPool = pool

  val colInfo = hc.getColInfo
  val getStock = HbaseContext.dataHandle
  val executorPool = hc.broadcastPool
  val stockCode = hc.broadcastSource[mutable.HashSet[String]](stock)
  var lastUpdateTime = 0
  var isFirstLaunch = true

  var prevRdd: RDD[(String, Int)] = null

  val accum = hc.accum[(String, Int)]("0", 0)

  /**
    * 每次提交时执行的逻辑
    *
    * @author wukun
    */
  override def run() {

    val cal: Calendar = Calendar.getInstance
    val nowUpdateTime = TimeHandle.getDay
    val month = TimeHandle.getMonth(cal, 1)
    val day = TimeHandle.getDay(cal)
    var nowTime = TimeHandle.maxStamp
    val prevTime = nowTime - 3600000

    if (nowUpdateTime != lastUpdateTime && TimeHandle.getNowHour(cal) == 0) {
      Stock.initFollowStockAlias(masterPool)
      RddOpt.updateAccum(masterPool.getConnect, "stock_follow", 0)
      lastUpdateTime = nowUpdateTime
    }

    try {
      dataManage(prevTime, nowTime, month, day)
    } catch {

      case e: RetriesExhaustedException =>
        HeatLogger.exception(e)

      case e: TableNotFoundException =>
        HeatLogger.exception(e)
        System.exit(-1)

      case e: Exception =>
        HeatLogger.exception(e)

    }

  }

  def dataManage(prevTime: Long, nowTime: Long, month: Int, day: Int) {

    if (isFirstLaunch) {

      isFirstLaunch = false

      hc.changeScan(prevTime - 3600000, prevTime)
      hc.changeConf()

      prevRdd = hc.generateRDD
        .map(_._2)
        .flatMap(x => {

          val value = Bytes.toString(x.getValue(Bytes.toBytes(colInfo._1),
            Bytes.toBytes(colInfo._2)))

          getStock(value)
        }).filter(x => {

        if (stockCode.value (x._1)) {
          true
        } else {
          false
        }

      }).reduceByKey(_ + _)
    }


    hc.changeScan(prevTime, nowTime)
    hc.changeConf()

    val timeTamp = nowTime / 1000

    val nowFollowData = hc.generateRDD
      .map(_._2)
      .persist(StorageLevel.MEMORY_AND_DISK)

    /* 记录一次更新有多少用户数 */
    val userCount = nowFollowData.count

    val sourceData = nowFollowData.flatMap(x => {

      val value = Bytes.toString(
        x.getValue(Bytes.toBytes(colInfo._1),
          Bytes.toBytes(colInfo._2)))
      getStock(value)

    })

    nowFollowData.unpersist()

    val stockCount = sourceData.filter(x => {

      if (stockCode.value (x._1)) {
        true
      } else {
        false
      }

    }).reduceByKey(_ + _).persist(StorageLevel.MEMORY_AND_DISK)

    stockCount.foreachPartition(x => {

      executorPool.value.getConnect match {

        case Some(connect) =>

          val mysqlHandle = MysqlHandle(connect)

          x.foreach(y => {

            mysqlHandle.execInsertInto(
              MixTool.insertCount("stock_follow", y._1, timeTamp, y._2)
            ) recover {
              case e: Exception => HeatLogger.exception(e)
            }

            mysqlHandle.execInsertInto(
              MixTool.insertOldCount("stock_follow_old", y._1, timeTamp, y._2)
            ) recover {
              case e: Exception => HeatLogger.exception(e)
            }

            mysqlHandle.execInsertInto(
              MixTool.updateAccum("stock_follow_accum", y._1, y._2)
            ) recover {
              case e: Exception => HeatLogger.exception(e)
            }

            accum += y
          })

          mysqlHandle.close()

        case None => HeatLogger.warn("[Get connect failure]")
      }
    })

    RddOpt.updateMax(masterPool.getConnect, "stock_max", "max_f", accum.value._2)
    accum.setValue("0", 0)

    if (prevRdd == null) {
      stockCount.foreachPartition(x => {

        executorPool.value.getConnect match {
          case Some(connect) =>

            val mysqlHandle = MysqlHandle(connect)

            x.foreach(y => {

              mysqlHandle.execInsertInto(
                {
                  MixTool.insertCount("stock_follow_add", y._1, timeTamp, y._2)
                }
              ) recover {
                case e: Exception => HeatLogger.exception(e)
              }

              mysqlHandle.execInsertInto(
                {
                  MixTool.updateMonthAccum("stock_follow_month_", y._1, month, day, y._2)
                }
              ) recover {
                case e: Exception => HeatLogger.exception(e)
              }
            })

            mysqlHandle.close()

          case None => HeatLogger.warn("[Get connect failure]")
        }
      })
    } else {
      stockCount.fullOuterJoin[Int](prevRdd).foreachPartition(x => {

        executorPool.value.getConnect match {
          case Some(connect) =>

            val mysqlHandle = MysqlHandle(connect)

            x.foreach(y => {
              val now = y._2._1 match {
                case Some(z) => z
                case None => 0
              }

              val prev = y._2._2 match {
                case Some(z) => z
                case None => 0
              }

              mysqlHandle.execInsertInto(
                MixTool.insertCount("stock_follow_add", y._1, timeTamp, now - prev)
              ) recover {
                case e: Exception => HeatLogger.exception(e)
              }

              mysqlHandle.execInsertInto(
                MixTool.updateMonthAccum("stock_follow_month_", y._1, month, day, now - prev)
              ) recover {
                case e: Exception => HeatLogger.exception(e)
              }
            })

            mysqlHandle.close()

          case None => HeatLogger.error("get connection failure")
        }
      })
    }

    prevRdd = stockCount

    /* 计算所有股票关注的总次数 */
    val allCount = stockCount.map(x => x._2).fold(0)((y, z) => y + z)

    if (allCount > 0) {

      masterPool.getConnect match {

        case Some(connect) =>
          val mysqlHandle = MysqlHandle(connect)

          mysqlHandle.execInsertInto(
            MixTool.insertTotal("stock_follow_count", timeTamp, allCount)
          ) recover {
            case e: Exception => HeatLogger.exception(e)
          }

          mysqlHandle.execInsertInto(
            MixTool.insertTime("update_follow", timeTamp)
          ) recover {
            case e: Exception => HeatLogger.exception(e)
          }

          mysqlHandle.close()

        case None => HeatLogger.warn("[Get connect failure]")
      }
    }

    val userWriter = Try(new FileWriter(path, true)) match {
      case Success(write) => write
      case Failure(e) => System.exit(-1)
    }

    val writer = userWriter.asInstanceOf[FileWriter]
    writer.write(timeTamp + ":" + userCount + "\n")
    writer.close()
  }
}

/**
  * Created by wukun on 2016/5/23
  * 伴生对象
  */
object TimerHandle {

  def apply(
             hc: HbaseContext,
             pool: MysqlPool,
             stock: mutable.HashSet[String],
             path: String,
             timeTamp: Long): TimerHandle = {
    new TimerHandle(hc, pool, stock, path, timeTamp)
  }

  def work(
            hc: HbaseContext,
            pool: MysqlPool,
            stock: mutable.HashSet[String],
            path: String,
            timeTamp: Long) {

    val timerHandle: Timer = new Timer
    val computeTime: Calendar = Calendar.getInstance
    val hour = TimeHandle.getHour(computeTime)
    TimeHandle.setTime(computeTime, hour, 0, 0, 0)
    timerHandle.scheduleAtFixedRate(TimerHandle(hc, pool, stock, path, timeTamp), computeTime.getTime, 60 * 60 * 1000)
  }
}
