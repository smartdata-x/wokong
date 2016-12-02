package com.kunyan.wokongsvc.person

/**
  * Created by sijiansheng on 2016/11/16.
  */
object TestResource {

  def main(args: Array[String]) {
    println(getClass.getResource("/log4j").getPath)
  }
}
