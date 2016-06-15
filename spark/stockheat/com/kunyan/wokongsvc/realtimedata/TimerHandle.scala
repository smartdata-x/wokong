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

 import org.apache.hadoop.conf.Configuration
 import org.apache.hadoop.hbase.util.Bytes
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
 import org.apache.spark.rdd.RDD

 /** 
   * Created by wukun on 2016/5/23
   * 定时任务实现类, 每隔5分钟提交一次作业
   */
 class TimerHandle(
   hbaseContext: HbaseContext, 
   pool: MysqlPool,
   stock: HashSet[String]
 ) extends TimerTask with Serializable with CustomLogger {

   @transient val hc = hbaseContext
   @transient val masterPool = pool
   val path = "/home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/"

   val colInfo = hc.getColInfo
   val getStock = HbaseContext.dataHandle
   val executorPool = hc.broadcastPool
   val stockCode = hc.broadcastSource[HashSet[String]](stock)
   var lastUpdateTime = 0

   var prevRdd: RDD[(String, Int)] = null

   val accum = hc.accum[(String, Int)]("0", 0)

   /**
     * 每次提交时执行的逻辑
     * @author wukun
     */
   override def run() {

     val nowUpdateTime = TimeHandle.getDay
     if(nowUpdateTime != lastUpdateTime && TimeHandle.getNowHour == 0) {

       RddOpt.updateAccum(masterPool.getConnect, "stock_follow", 0)
       lastUpdateTime = nowUpdateTime
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

     val stockCount = sourceData.filter( x => {
       if((stockCode.value)(x._1)) {
         true
       } else {
         false
       }
     }).reduceByKey(_ + _).persist(StorageLevel.MEMORY_AND_DISK)

     stockCount.foreachPartition( x => {

       executorPool.value.getConnect match {
         case Some(connect) => {

           val mysqlHandle = MysqlHandle(connect)

           x.foreach( y => {

             mysqlHandle.execInsertInto(
               MixTool.insertCount("stock_follow", y._1, timeTamp, y._2)
             ) recover {
               case e: Exception => warnLog(fileInfo, e.getMessage + "[Update data failure]")
             }

             mysqlHandle.execInsertInto(
               MixTool.insertOldCount("stock_follow_old", y._1, timeTamp, y._2)
             ) recover {
               case e: Exception => warnLog(fileInfo, e.getMessage + "[Update old data failure]")
             }

             mysqlHandle.execInsertInto(
               MixTool.updateAccum("stock_follow_accum", y._1, y._2)
             ) recover {
               case e: Exception => warnLog(fileInfo, e.getMessage + "[Update accum failure]")
             }

             accum += y
           })

           mysqlHandle.close
         }

         case None => warnLog(fileInfo, "[Get connect failure]")
       }
     })

     RddOpt.updateMax(masterPool.getConnect, "stock_max", "max_f", accum.value._2)
     accum.setValue("0", 0)

     if(prevRdd == null) {
       stockCount.foreachPartition( x => {
         RddOpt.updateAddFirst(executorPool.value.getConnect, x, "stock_follow_add", timeTamp)
       })
     } else {
       stockCount.fullOuterJoin[Int](prevRdd).foreachPartition( x => {
         RddOpt.updateAdd(executorPool.value.getConnect, x, "stock_follow_add", timeTamp)
       })
     }

     prevRdd = stockCount

     /* 计算所有股票关注的总次数 */
     val allCount = stockCount.map( x => x._2 ).fold(0)( (y, z) => y + z)

     if(allCount > 0) {

       masterPool.getConnect match {

         case Some(connect) => {
           val mysqlHandle = MysqlHandle(connect)

           mysqlHandle.execInsertInto(
             MixTool.insertTotal("stock_follow_count", timeTamp, allCount)
           ) recover {
             case e: Exception => warnLog(fileInfo, e.getMessage + "[Update all_data failure]")
           }

           mysqlHandle.execInsertInto(
             MixTool.insertTime("update_follow", timeTamp)
           ) recover {
             case e: Exception => warnLog(fileInfo, e.getMessage + "[Update time failure]")
           }

           mysqlHandle.close
         }

         case None => warnLog(fileInfo, "[Get connect failure]")
       }
     }

    /* 初始化写文件句柄 */
    val userWriter = Try(new FileWriter("/home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/userStatic.txt", true)) match {
      case Success(write) => write
      case Failure(e) => System.exit(-1)
    }

    val writer = userWriter.asInstanceOf[FileWriter]
    writer.write(timeTamp + ":" + userCount + "\n")
    writer.close
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
     stock: HashSet[String]): TimerHandle = {
     new TimerHandle(hc, pool, stock)
   }

   def work(
     hc: HbaseContext, 
     pool: MysqlPool, 
     stock: HashSet[String]) {

     val timerHandle:Timer = new Timer
     val computeTime:Calendar = Calendar.getInstance
     val hour = TimeHandle.getHour(computeTime)
     TimeHandle.setTime(computeTime, hour, 0, 0, 0)
     timerHandle.scheduleAtFixedRate(TimerHandle(hc, pool, stock), computeTime.getTime(), 60 * 60 * 1000)
   }
 }
