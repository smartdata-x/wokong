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

import MixTool.TupleHashMapSet

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

    if(args.length != 4) {
      errorLog(fileInfo, "args too little")
      System.exit(-1)
    }

    /* 加载日志配置 */
    PropertyConfigurator.configure(args(0))

    /* 初始化xml配置 */
    val xml = XmlHandle(args(1))

    /* 初始化mysql连接池 */
    val mysqlPool = MysqlPool(xml)
    mysqlPool.setConfig(1, 2)

    /* 初始化A股下公司用到的股票代码 */
    val stockInfo = mysqlPool.getConnect match {

      case Some(connect) => {

        val sqlHandle = MysqlHandle(connect)

        val stock = sqlHandle.execQueryStock(MixTool.STOCK_SQL) match {

          case Success(z) => z
          case Failure(e) => {
            errorLog(fileInfo, e.getMessage + "[Query stock exception]")
            System.exit(-1)
          }

        }

        val stockHyGn = sqlHandle.execQueryHyGn(MixTool.STOCK_HY_GN) recover {

          case e: Exception => {
            errorLog(fileInfo, e.getMessage + "[initial stock_hy_gn exception]")
            System.exit(-1)
          }

        }

        sqlHandle.close

        (stock, stockHyGn)
      }

      case None => {
        errorLog(fileInfo, "[Get mysql connect failure]")
        System.exit(-1)
      }
    }

    val molt = stockInfo.asInstanceOf[(HashSet[String], Try[TupleHashMapSet])]
    val hbaseContext = HbaseContext(xml)

    TimerHandle.work(hbaseContext, mysqlPool, molt._1, molt._2.get, args(2))
  }
}
