package com.kunyan.log

import org.apache.log4j.{PropertyConfigurator, BasicConfigurator, Logger}

/**
  * Created by Administrator on 2016/1/26.
  */
object HWLogger {

  val logger = Logger.getLogger("HOT_WORDS")
  BasicConfigurator.configure()
  PropertyConfigurator.configure("/home/hotwords/conf/log4j.properties")

  def exception(e: Exception) = {
    logger.error(e.printStackTrace())
  }

  def error(msg: String): Unit = {
    logger.error(msg)
  }

  def warn(msg: String): Unit = {
    logger.warn(msg)
  }

  def info(msg: String): Unit = {
    logger.info(msg)
  }

  def debug(msg: String): Unit = {
    logger.debug(msg)
  }

}
