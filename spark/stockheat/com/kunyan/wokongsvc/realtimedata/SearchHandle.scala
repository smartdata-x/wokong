/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/SearchHandle.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-06-01 20:11
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import com.kunyan.wokongsvc.realtimedata.MixTool.Tuple2Map

import CustomAccum._

import java.util.Timer
import java.util.TimerTask
import java.util.Calendar
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import org.apache.spark.storage.StorageLevel
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD 

/** 
 * Created by wukun on 2016/06/01
 * 定时任务实现类, 每隔5分钟提交一次作业
 */
class SearchHandle(
  fileContext: FileContext,
  pool: MysqlPool,
  stockAlias: Tuple2Map
) extends TimerTask with Serializable with CustomLogger {

  @transient val fc = fileContext 

  @transient val masterPool = pool
  val executorPool = fc.broadcastPool

  val alias = fc.broadcastSource[Tuple2Map](stockAlias)

  var prevRdd: RDD[((String, String), Int)] = null
  val accum = fc.accum[(String, Int)](("0", 0))
  var lastUpdateTime = 0

  /**
   * 每次提交时执行的逻辑
   * @author wukun
   */
  override def run() {

    val nowUpdateTime = TimeHandle.getDay
    if(nowUpdateTime != lastUpdateTime && TimeHandle.getNowHour == 0) {
      RddOpt.updateAccum(masterPool.getConnect, "stock_search", 0)
      lastUpdateTime = nowUpdateTime
    }

    val timeTamp = TimeHandle.getTamp(fc.getTamp)

    val dataRdd = fc.generateRdd.map( x => {
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
          RddOpt.updateCount(fileInfo, executorPool.value.getConnect, x, accum, MixTool.SEARCH, timeTamp)
        })

        RddOpt.updateMax(masterPool.getConnect, "stock_max", "max_s", accum.value._2)
        accum.setValue("0", 0)

        if(prevRdd == null) {
          eachCodeCount.map( x => (x._1._1, x._2)).foreachPartition( y => {
            RddOpt.updateAddFirst(executorPool.value.getConnect, y, "stock_search_add", timeTamp)
          })
        } else {
          eachCodeCount.fullOuterJoin[Int](prevRdd).map( x=> (x._1._1, x._2)).foreachPartition( y => {
            RddOpt.updateAdd(executorPool.value.getConnect, y, "stock_search_add", timeTamp)
          })
        }

        prevRdd = eachCodeCount

        eachCodeCount.map( y => {
          (y._1._2, y._2)
        }).reduceByKey(_ + _).foreach( z => {
          RddOpt.updateTotal(fileInfo, executorPool.value.getConnect, MixTool.ALL_SEARCH, timeTamp, z._2)
        })

        RddOpt.updateTime(masterPool.getConnect, "update_search", timeTamp)
      }

      case Failure(e) => e
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
    alias: Tuple2Map): SearchHandle = {
      new SearchHandle(fc, pool, alias)
  }

  def work(
    fc: FileContext, 
    pool: MysqlPool,
    alias: Tuple2Map) {
      val timerHandle:Timer = new Timer
      val computeTime:Calendar = Calendar.getInstance
      val hour = TimeHandle.getHour(computeTime)
      TimeHandle.setTime(computeTime, hour, 0, 0, 0)
      timerHandle.scheduleAtFixedRate(SearchHandle(fc, pool, alias), computeTime.getTime(), 60 * 60 * 1000)
  }
}
