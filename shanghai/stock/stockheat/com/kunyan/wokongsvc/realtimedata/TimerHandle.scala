/*=============================================================================
  #    Copyright (c) 2015
  #    ShanghaiKunyan.  All rights reserved
  #
  #    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/TimerHandle.scala
  #    Author       : Sunsolo
  #    Email        : wukun@kunyan-inc.com
  #    Date         : 2016-05-22 16:04
  #    Description  : 
  =============================================================================*/

 package com.kunyan.wokongsvc.realtimedata

 import com.kunyan.wokongsvc.realtimedata.CustomAccum._
 import MixTool.TupleHashMapSet

 import org.apache.hadoop.conf.Configuration
 import org.apache.hadoop.hbase.util.Bytes
 import org.apache.spark.rdd.RDD
 import org.apache.spark.storage.StorageLevel

 import java.io.FileWriter
 import java.util.Calendar
 import java.util.Timer
 import java.util.TimerTask
 import scala.collection.mutable.HashSet
 import scala.collection.mutable.HashMap
 import scala.collection.mutable.ListBuffer
 import scala.util.Try
 import scala.util.Success
 import scala.util.Failure

 /** 
   * Created by wukun on 2016/5/23
   * 定时任务实现类, 每隔5分钟提交一次作业
   */
 class TimerHandle(
   hbaseContext: HbaseContext, 
   pool: MysqlPool,
   stock: HashSet[String],
   stockHyGn: TupleHashMapSet,
   path: String
 ) extends TimerTask with Serializable with CustomLogger {

   @transient val hc = hbaseContext

   @transient val masterPool = pool

   val colInfo = hc.getColInfo

   val getStock = HbaseContext.dataHandle
   val executorPool = hc.broadcastPool
   val stockCode = hc.broadcastSource[HashSet[String]](stock)
   val stockHy = hc.broadcastSource(stockHyGn._1._1)
   val stockGn = hc.broadcastSource(stockHyGn._1._2)

   val accumATotal = hc.accum[Int](0)

   var lastUpdateTime = 0
   var prevRdd: RDD[(String, Int)] = null
   var prevHyGn: HashMap[String, Int] = null
   val hyGn = stockHyGn._2
   var temp: HashMap[String, Int] = new HashMap[String, Int]

   /**
     * 每次提交时执行的逻辑
     * @author wukun
     */
   override def run() {

     val cal: Calendar = Calendar.getInstance
     val month = TimeHandle.getMonth(cal, 1)
     val day = TimeHandle.getDay(cal)

     if(day != lastUpdateTime && TimeHandle.getNowHour(cal) == 0) {

       RddOpt.resetData(masterPool.getConnect, "proc_resetData")
       lastUpdateTime = day

     } else {
       RddOpt.resetDiffData(masterPool.getConnect, "proc_resetDiffData")
     }

     val monthTable = month%2 match {
       case 0 => ("s_v_month_prev", "hy_v_month_prev", "gn_v_month_prev")
       case 1 => ("s_v_month_now", "hy_v_month_now", "gn_v_month_now")
      }

     val nowTime = TimeHandle.maxStamp
     val prevTime = nowTime - 3600000

     hc.changeScan(prevTime, nowTime)
     hc.changeConf

     val timeTamp = nowTime / 1000

     val hbaseData = hc.generateRDD.map(_._2).persist(StorageLevel.MEMORY_AND_DISK)

     /* 记录一次更新有多少用户数 */
     val userCount = hbaseData.count

     val sourceData = hbaseData.flatMap( x => {
       val value = Bytes.toString(x.getValue(Bytes.toBytes(colInfo._1), 
         Bytes.toBytes(colInfo._2)))
       getStock(value)
     })

     val eachCodeCount = sourceData.filter( x => {

       if((stockCode.value)(x._1)) {
         true
       } else {
         false
       }

     }).reduceByKey(_ + _).persist(StorageLevel.MEMORY_AND_DISK)

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

     val userWriter = Try(new FileWriter(path, true)) match {
       case Success(write) => write
       case Failure(e) => System.exit(-1)
     }
   
     val writer = userWriter.asInstanceOf[FileWriter]
     writer.write(timeTamp + ":" + userCount + "\n")
     writer.close

     accumATotal.setValue(0)
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
     stock: HashSet[String],
     stockHyGn: TupleHashMapSet,
     path: String): TimerHandle = {
     new TimerHandle(hc, pool, stock, stockHyGn, path)
   }

   def work(
     hc: HbaseContext, 
     pool: MysqlPool, 
     stock: HashSet[String],
     stockHyGn: TupleHashMapSet,
     path: String) {

     val TIMERHANDLE: Timer = new Timer
     val COMPUTETIME: Calendar = Calendar.getInstance
     val HOUR = TimeHandle.getHour(COMPUTETIME)

     TimeHandle.setTime(COMPUTETIME, HOUR, 0, 0, 0)
     TIMERHANDLE.scheduleAtFixedRate(TimerHandle(hc, pool, stock, stockHyGn, path), COMPUTETIME.getTime(), 60 * 60 * 1000)
   }
 }
