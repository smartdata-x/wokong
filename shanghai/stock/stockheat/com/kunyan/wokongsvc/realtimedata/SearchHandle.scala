/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/SearchHandle.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-06-01 20:11
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import com.kunyan.wokongsvc.realtimedata.MixTool.Tuple2Map
import com.kunyan.scalautil.message.TextSender
import CustomAccum._

import org.apache.spark.rdd.RDD 
import org.apache.spark.storage.StorageLevel

import java.util.Calendar
import java.util.Timer
import java.util.TimerTask
import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/** 
 * Created by wukun on 2016/06/01
 * 定时任务实现类, 每隔5分钟提交一次作业
 */
class SearchHandle(
  fileContext: FileContext,
  pool: MysqlPool,
  stockAlias: Tuple2Map,
  path: String
) extends TimerTask with Serializable with CustomLogger {

  @transient val fc = fileContext 
  @transient val masterPool = pool

  val executorPool = fc.broadcastPool
  val alias = fc.broadcastSource[Tuple2Map](stockAlias)

  var prevRdd: RDD[((String, String), Int)] = null
  val accum = fc.accum[(String, Int)](("0", 0))
  var isAlarm: Int = 0
  var prevDay = 0
  var isEnd = ""
  var isSameHour = 0

  /**
   * 每次提交时执行的逻辑
   * @author wukun
   */
  override def run() {

    val timeTamp: String = fc.getTamp
    val timeRef = getFileDate(timeTamp)

    val nowHour = timeRef._1._3
    val nowDay  = timeRef._1._2
    val nowMonth= timeRef._1._1

    if(nowDay != prevDay && nowHour == 0 && nowHour != isSameHour) {

      RddOpt.updateAccum(masterPool.getConnect, "stock_search", 0)
      prevDay = nowDay

    }

    if(nowHour != isSameHour) {
      dataManage(nowDay, timeRef._2, nowMonth, true, "shdx_" + timeTamp)
      isSameHour = nowHour
    }

    while({

      isEnd = fc.getLazyFile(path)
      isEnd

    } != null) {

      val timeRel = getFileDate(isEnd.substring(5))

      if(prevDay == timeRel._1._2) {
        dataManage(timeRel._1._2, timeRel._2, timeRel._1._1, false, isEnd)
      } else {
        dataManage(isEnd, (timeRel._1._1, timeRel._1._2, timeRel._2))
      }
    } 

  }

  def getFileDate(fileTimeTamp: String): ((Int, Int, Int), Long) = {

    val date     = TimeHandle.getTamp(fileTimeTamp)
    val now_day  = date.getDate
    val timeTamp = date.getTime / 1000
    val month    = date.getMonth + 1
    val hour     = date.getHours

    ((month, now_day, hour), timeTamp)
  }

  def dataManage(day: Int, timeTamp: Long, month: Int, is_now: Boolean, fileName: String) {

    val dataRdd = fc.generateRdd(fileName).map( x => {

      MixTool.stockClassify(x, alias.value)

    }).filter( x => {

      if(x._1._2.compareTo("0") == 0 || x._1._2.compareTo("1") == 0) {
        false
      } else {
        true
      }

    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    Try(dataRdd.take(1)) match {

      case Success(z) => {

        val eachCodeCount = dataRdd.map( (y: ((String,String),String)) => {
          (y._1, 1)
        }).reduceByKey(_ + _).persist(StorageLevel.MEMORY_AND_DISK_SER)

        eachCodeCount.foreachPartition( x => {

          executorPool.value.getConnect match {

            case Some(connect) => {

              val mysqlHandle = MysqlHandle(connect)

              RddOpt.updateCount(fileInfo, mysqlHandle, x, accum, MixTool.SEARCH, timeTamp, month, day)

              mysqlHandle.batchExec recover {
                case e: Exception => {
                  warnLog(fileInfo, "[exec updateCount failure]" + e.getMessage)
                }
              }

              mysqlHandle.close
            }

            case None => warnLog(fileInfo, "Get connect exception")
          }
        })

        RddOpt.updateMax(masterPool.getConnect, "stock_max", "max_s", accum.value._2)
        accum.setValue("0", 0)

        if(prevRdd == null) {
          eachCodeCount.map( x => (x._1._1, x._2)).foreachPartition( y => {

            executorPool.value.getConnect match {

              case Some(connect) => {

                val mysqlHandle = MysqlHandle(connect)

                RddOpt.updateAddFirst(mysqlHandle, y, "stock_search_add", timeTamp)

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

          eachCodeCount.fullOuterJoin[Int](prevRdd).map( x=> (x._1._1, x._2)).foreachPartition( y => {

            executorPool.value.getConnect match {

              case Some(connect) => {

                val mysqlHandle = MysqlHandle(connect)

                RddOpt.updateAdd(mysqlHandle, y, "stock_search_add", timeTamp)

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
          RddOpt.updateTotal(fileInfo, executorPool.value.getConnect, MixTool.ALL_SEARCH, timeTamp, z._2)
        })

        RddOpt.updateTime(masterPool.getConnect, "update_search", timeTamp)

        isAlarm += 1
      }

      case Failure(e) => {
        e.getMessage
      }
    }

    if(isAlarm == 0 && is_now) {
      TextSender.send("C4545CC1AE91802D2C0FBA7075ADA972", fc.getTamp + "时刻的离线搜索数据不存在或大小为0", "18106557417,18817511172,15026804656,18600397635")
    }

    isAlarm = 0
  }

  def dataManage(fileName: String, time: (Int, Int, Long)) {

    val dataRdd = fc.generateRdd(fileName).map( x => {

      MixTool.stockClassify(x, alias.value)

    }).filter( x => {

      if(x._1._2.compareTo("0") == 0 || x._1._2.compareTo("2") == 0) {
        false
      } else {
        true
      }

    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    Try(dataRdd.take(1)) match {

      case Success(z) => {

        val eachCodeCount = dataRdd.map( (y: ((String,String),String)) => {
          (y._1, 1)
        }).reduceByKey(_ + _).persist(StorageLevel.MEMORY_AND_DISK_SER)

        eachCodeCount.foreachPartition( x => {

          executorPool.value.getConnect match {

            case Some(connect) => {

              val mysqlHandle = MysqlHandle(connect)

              RddOpt.updateCount(fileInfo, mysqlHandle, x, MixTool.SEARCH, time._3, time._1, time._2) 

              mysqlHandle.batchExec recover {
                case e: Exception => {
                  warnLog(fileInfo, "[exec updateCount failure]" + e.getMessage)
                }
              }

              mysqlHandle.close
            }

            case None => warnLog(fileInfo, "Get connect exception")
          }
        })
      }

      case Failure(e) => {
        e.getMessage
      }
    }
  }
}

/**
 * Created by wukun on 2016/06/01
 * 伴生对象
 */
object SearchHandle {

  def apply(
    fc: FileContext, 
    pool: MysqlPool, 
    alias: Tuple2Map,
    path: String): SearchHandle = {
      new SearchHandle(fc, pool, alias, path)
  }

  def work(
    fc: FileContext, 
    pool: MysqlPool,
    alias: Tuple2Map,
    path: String) {

      val timerHandle:Timer = new Timer
      val computeTime:Calendar = Calendar.getInstance
      val hour = TimeHandle.getHour(computeTime)
      TimeHandle.setTime(computeTime, hour, 0, 0, 0)
      timerHandle.scheduleAtFixedRate(SearchHandle(fc, pool, alias, path), computeTime.getTime(), 30 * 60 * 1000)
  }
}
