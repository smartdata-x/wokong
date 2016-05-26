/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/TestImplicit.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-05-24 14:41
#    Description  : 
=============================================================================*/
package com.kunyan.wokongsvc.realtimedata

class ObjectImplicit {
  val content = "Yes"
  def addText(x: String): String = {
    content + x
  }
}

object TestImplicit{

  implicit object objectImplicit extends ObjectImplicit {
  }

  def test(implicit impl: ObjectImplicit) {
    println(impl.addText(" I LOVE YOu"))
  }

  def main(args: Array[String]) {
    test
  }
}
