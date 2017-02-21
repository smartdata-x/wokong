/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/ConsumerMain.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-08-25 14:24
#    Description  :
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

object ConsumerMain {

  def main(args: Array[String]) {

    val xmlHandle = XmlHandle(args(0))
    val mysqlPool = MysqlPool(xmlHandle)
    val consumer = new KafkaConsumer

    val consumerContext = ConsumerContext(mysqlPool, consumer)

    consumerContext.doWork()
    sys.addShutdownHook(consumerContext.shutdown())
  }
}

