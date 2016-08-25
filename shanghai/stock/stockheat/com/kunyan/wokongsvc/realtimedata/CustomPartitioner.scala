/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/CustomPartitioner.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-08-24 20:54
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import kafka.producer.Partitioner
import kafka.utils.VerifiableProperties

class CustomPartitioner(props: VerifiableProperties = null) extends Partitioner {

  def partition(key: Any, numPartitions: Int): Int = {
    key.asInstanceOf[String].toInt % numPartitions
  }
}



