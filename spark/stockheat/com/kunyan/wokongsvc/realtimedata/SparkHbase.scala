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

/**
  * Created by wukun on 2016/5/23
  * Hbase操作主程序入口
  */
object SparkHbase {

  def main(args:Array[String]) {
    val xml = XmlHandle("./config.xml")
    val mysqlPool = MysqlPool(xml)
    mysqlPool.setConfig(1, 2)

    val hbaseContext = HbaseContext(xml)

    TimerHandle.work(hbaseContext, mysqlPool)
  }
}
