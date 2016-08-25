/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/SparkHbase.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-05-23 12:07
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import org.apache.log4j.PropertyConfigurator

import java.io.FileWriter
import java.io.IOException
import scala.collection.mutable.HashSet
import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
  * Created by wukun on 2016/5/23
  * Hbase操作主程序入口
  */
object SparkHbase extends CustomLogger {

  def main(args: Array[String]) {

    args foreach println
    if(args.length != 5) {
      errorLog(fileInfo, "args too little")
      System.exit(-1)
    }

    /* 加载日志配置 */
    PropertyConfigurator.configure(args(0))

    /* 初始化xml配置 */
    val xml = XmlHandle(args(1))

    /* 初始化mysql连接池 */
    val mysqlPool = MysqlPool(xml)
    mysqlPool.setConfig(1, 2, 3)

    /* 初始化A股下公司用到的股票代码 */
    val stockCode = mysqlPool.getConnect match {
      case Some(connect) => {
        val sqlHandle = MysqlHandle(connect)

        val stock = sqlHandle.execQueryStock(MixTool.STOCK_SQL) match {
          case Success(z) => z
          case Failure(e) => {
            errorLog(fileInfo, e.getMessage + "[Query stock exception]")
            System.exit(-1)
          }
        }
        sqlHandle.close

        stock
      }

      case None => {
        errorLog(fileInfo, "[Get mysql connect failure]")
        System.exit(-1)
      }
    }

    /*val stockWriter = Try(new FileWriter(args(3), true)) match {
      case Success(write) => write
      case Failure(e) => System.exit(-1)
    } */

    val hbaseContext = HbaseContext(xml)

    TimerHandle.work(hbaseContext, mysqlPool, 
      stockCode.asInstanceOf[HashSet[String]], args(2), args(4).toLong)
  }
}
