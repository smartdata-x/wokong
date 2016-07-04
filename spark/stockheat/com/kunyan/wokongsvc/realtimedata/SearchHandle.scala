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

import com.kunyan.wokongsvc.realtimedata.MixTool.{Tuple2Map, TupleHashMapSet}

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
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

/** 
 * Created by wukun on 2016/06/01
 * 定时任务实现类, 每隔5分钟提交一次作业
 */
class SearchHandle(
  fileContext: FileContext,
  pool: MysqlPool,
  stockAlias: Tuple2Map,
  hyGnInfo: TupleHashMapSet
) extends TimerTask with Serializable with CustomLogger {

  @transient val fc = fileContext 

  @transient val masterPool = pool
  val executorPool = fc.broadcastPool

  val alias = fc.broadcastSource(stockAlias)
  val stockHy = fc.broadcastSource(hyGnInfo._1._1)
  val stockGn = fc.broadcastSource(hyGnInfo._1._2)
  val hyGn = hyGnInfo._2

  /* 初始化计算某时刻A股访问总量；累加器 */
  val accumATotal = fc.accum[Int](0)

  var prevRdd: RDD[(String, Int)] = null
  var prevHyGn: HashMap[String, Int] = null
  var temp: HashMap[String, Int] = new HashMap[String, Int]
  var lastUpdateTime = 0

  /**
   * 每次提交时执行的逻辑
   * @author wukun
   */
  override def run() {

    val date = TimeHandle.getTamp(fc.getTamp)
    val month = date.getMonth + 1
    val day = date.getDate

    if(day != lastUpdateTime && date.getHours == 0) {

      RddOpt.resetData(masterPool.getConnect, "proc_resetData")
      lastUpdateTime = day

    } else {
      RddOpt.resetDiffData(masterPool.getConnect, "proc_resetDiffData")
    }

    val monthTable = month%2 match {
      case 0 => ("s_v_month_prev", "hy_v_month_prev", "gn_v_month_prev")
      case 1 => ("s_v_month_now", "hy_v_month_now", "gn_v_month_now")
    }

    val timeTamp = date.getTime / 1000

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
          (y._1._1, 1)
        }).reduceByKey(_ + _).persist(StorageLevel.MEMORY_AND_DISK_SER)

        if(prevRdd == null) {
          eachCodeCount.map( x => (x._1, x._2)).foreachPartition( y => {
            RddOpt.updateADataFirst(executorPool.value.getConnect, y, "proc_visit_A_data", timeTamp, monthTable._1, day, accumATotal)
          })
        } else {
          eachCodeCount.fullOuterJoin[Int](prevRdd).map( x=> (x._1, x._2)).foreachPartition( y => {
            RddOpt.updateAData(executorPool.value.getConnect, y, "proc_visit_A_data", timeTamp, monthTable._1, "s_v_diff", day, accumATotal)
          })
        }

        prevRdd = eachCodeCount

        val nowHyGnData = eachCodeCount.flatMap( x => {

            /* gnInfo保存的是单只股票对应的所有概念 */
           val gnInfo = stockGn.value.getOrElse(x._1, new ArrayBuffer[String])

            /* hyInfo保存的是单只股票对应的所有行业 */
           val hyInfo = stockHy.value.getOrElse(x._1, new ArrayBuffer[String])

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

        RddOpt.updateTotalTime(masterPool.getConnect, timeTamp, accumATotal.value)
      }

      case Failure(e) => e
    }

    accumATotal.setValue(0)
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
    hyGn: TupleHashMapSet): SearchHandle = {
      new SearchHandle(fc, pool, alias, hyGn)
  }

  def work(
    fc: FileContext, 
    pool: MysqlPool,
    alias: Tuple2Map,
    hyGn: TupleHashMapSet) {

      val timerHandle:Timer = new Timer
      val computeTime:Calendar = Calendar.getInstance
      val hour = TimeHandle.getHour(computeTime)

      TimeHandle.setTime(computeTime, hour, 0, 0, 0)
      timerHandle.scheduleAtFixedRate(SearchHandle(fc, pool, alias, hyGn), computeTime.getTime(), 60 * 60 * 1000)
  }
}
