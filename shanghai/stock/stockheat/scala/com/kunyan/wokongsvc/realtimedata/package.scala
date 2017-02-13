package com.kunyan.wokongsvc

import org.apache.log4j.{PropertyConfigurator, BasicConfigurator, Logger}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  * Created by sijiansheng on 2016/11/21.
  */
package object realtimedata {

  val logger = Logger.getLogger("LoggerTest")
  BasicConfigurator.configure()
  //  PropertyConfigurator.configure("/home/stockheat/conf/log4j.properties")
  PropertyConfigurator.configure("E://log4j.properties")


  type TryHashMap = Try[mutable.HashMap[String, (String, String)]]
  type TupleHashMap = (mutable.HashMap[String, ListBuffer[String]], mutable.HashMap[String, ListBuffer[String]])
  type TryTuple3HashMap = Try[(mutable.HashSet[String], (mutable.HashMap[String, String], mutable.HashMap[String, String], mutable.HashMap[String, String]))]

  def exception[U <: Throwable](e: U) = {
    logger.error(e.getLocalizedMessage)
    logger.error(e)
    logger.error(e.getStackTraceString)
  }
}
