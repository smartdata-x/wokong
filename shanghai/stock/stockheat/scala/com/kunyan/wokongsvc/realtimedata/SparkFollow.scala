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

/**
  * Created by wukun on 2016/5/23
  * Hbase操作主程序入口
  */
object SparkFollow {

  def main(args: Array[String]) {

    logger.info("开始执行")
    /* 加载日志配置 */
    PropertyConfigurator.configure(args(0))
    
    if (args.length != 5) {
      logger.error("args too little")
      System.exit(-1)
    }

    /* 初始化xml配置 */
    val xml = XmlHandle(args(1))

    /* 初始化mysql连接池 */
    val mysqlPool = MysqlPool(xml)
    mysqlPool.setConfig(1, 2, 3)

    /* 初始化A股下公司用到的股票代码 */
    Stock.initFollowStockAlias(mysqlPool)

    /*val stockWriter = Try(new FileWriter(args(3), true)) match {
      case Success(write) => write
      case Failure(e) => System.exit(-1)
    } */

    val hbaseContext = HbaseContext(xml)

    TimerHandle.work(hbaseContext, mysqlPool,
      Stock.followStockAlias, args(2), args(4).toLong)
  }
}
