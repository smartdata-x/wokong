/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/SparkFile.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-06-01 20:51
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import org.apache.log4j.PropertyConfigurator
import MixTool.Tuple2Map
import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
  * Created by wukun on 2016/06/01
  * 静态文件操作主入口
  */
object SparkFile extends CustomLogger {

  def main(args: Array[String]) {

    if(args.length != 2) {
      errorLog(fileInfo, "args too little")
      System.exit(-1)
    }

    /* 加载日志配置文件 */
   PropertyConfigurator.configure(args(0))

   val xmlHandle = XmlHandle(args(1))

   /* 初始化mysql连接池 */
   val mysqlPool = MysqlPool(xmlHandle)
   mysqlPool.setConfig(1, 2)

   val stockalias = mysqlPool.getConnect match {
     case Some(connect) => {
       val sqlHandle = MysqlHandle(connect)

       val alias = sqlHandle.execQueryStockAlias(MixTool.SYN_SQL) match {
         case Success(z) => z
         case Failure(e) => {
           errorLog(fileInfo, e.getMessage + "[Query stockAlias exception]")
           System.exit(-1)
         }
       }
       sqlHandle.close


       alias
     }

     case None => {
       errorLog(fileInfo, "[Get mysql connect failure]")
       System.exit(-1)
     }
   }

   val fileContext = FileContext(xmlHandle)

   SearchHandle.work(fileContext, mysqlPool, stockalias.asInstanceOf[Tuple2Map])
  }
}

