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

 import org.apache.hadoop.conf.Configuration
 import org.apache.hadoop.hbase.util.Bytes

 import java.util.Calendar
 import java.util.Timer
 import java.util.TimerTask
 import scala.collection.mutable.ListBuffer

 /** 
   * Created by wukun on 2016/5/23
   * 定时任务实现类, 每隔5分钟提交一次作业
   */
 class TimerHandle(
   hbaseContext: HbaseContext, 
   pool: MysqlPool
 ) extends TimerTask with Serializable with CustomLogger {

   @transient val hc = hbaseContext
   @transient val masterPool = pool

   val colInfo = hc.getColInfo
   val getStock = HbaseContext.dataHandle
   val executorPool = hc.broadcastPool

   /**
     * 每次提交时执行的逻辑
     * @author wukun
     */
   override def run() {

     val nowTime = TimeHandle.maxStamp
     val prevTime = nowTime - 300000
     hc.changeScan(prevTime, nowTime)
     hc.changeConf

     val timeTamp = nowTime / 1000

     masterPool.getConnect match {
       case Some(connect) => {
         val mysqlHandle = MysqlHandle(connect)

         mysqlHandle.execInsertInto(
           MixTool.insertSql(timeTamp)
         ) recover {
           case e: Exception => warnLog(fileInfo, e.getMessage + "[Update time failure]")
         }

         mysqlHandle.close
       }

       case None => warnLog(fileInfo, "[Get connect failure]")
     }

     println(timeTamp)
     hc.generateRDD.map(_._2).flatMap( x => {
       val value = Bytes.toString(x.getValue(Bytes.toBytes(colInfo._1), 
         Bytes.toBytes(colInfo._2)))
       //println(value)
       getStock(value)
     }).reduceByKey(_ + _).foreachPartition( x => {

       executorPool.value.getConnect match {
         case Some(connect) => {

           val mysqlHandle = MysqlHandle(connect)

           x.foreach( y => {
             mysqlHandle.execInsertInto(
               MixTool.insertSql(y._1, timeTamp, y._2)
             ) recover {
                 case e: Exception => warnLog(fileInfo, e.getMessage + "[Update data failure]")
               }
           })

           mysqlHandle.close
         }
         case None => warnLog(fileInfo, "[Get connect failure]")
       }
     })
   }
 
 }

 /**
   * Created by wukun on 2016/5/23
   * 伴生对象
   */
 object TimerHandle {

   def apply(hc:HbaseContext, pool: MysqlPool):TimerHandle = {
     new TimerHandle(hc, pool)
   }

   def work(hc:HbaseContext, pool: MysqlPool) {

     val timerHandle:Timer = new Timer
     val computeTime:Calendar = Calendar.getInstance
     TimeHandle.setTime(computeTime, 1, 0, 0)
     timerHandle.scheduleAtFixedRate(TimerHandle(hc, pool), computeTime.getTime(), 5 * 60 * 1000)
   }
 }
