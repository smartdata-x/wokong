/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/ConsumerContext.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-08-25 13:15
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit

/**
  * Created by wukun on 2016/08/25
  * 消费上下文类
  */
class ConsumerContext(val pool: MysqlPool, val consumer: KafkaConsumer) {

  val executor = Executors.newFixedThreadPool(consumer.partitionTotal)

  def doWork {
    consumer.getStreams.foreach( x => {
      x._2.foreach( y => {
        executor.submit(HeatThread(y, pool))
      })
    })
  }

  /**
    * 资源的释放
    */
  def shutdown {

    pool.close
    consumer.shutdown
    executor.shutdown

    try {
      while(!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
      }
    } catch {
      case e: InterruptedException => {
        System.exit(-1)
      }
    }
  }

}

/**
  * Created by wukun on 2016/08/25
  * 消费上下文类伴生对象
  */
object ConsumerContext {

  def apply(
    pool: MysqlPool,
    consumer: KafkaConsumer
  ): ConsumerContext = {
    new ConsumerContext(pool, consumer)
  }
}

