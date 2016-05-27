package com.kunyan.data.log

import org.apache.log4j.Logger

/**
  * Created by C.J.YOU on 2016/5/27.
  */
object PrismLogger {

  private  val logger = Logger.getLogger("PrismLogger")

  def debug(msg: String): Unit = {
    logger.debug(msg)
  }

  def info(msg: String): Unit = {
    logger.info(msg)
  }

  def warn(msg: String): Unit = {
    logger.warn(msg + "<<<<=======")
  }

  def error(msg: String): Unit = {
    logger.error(msg)
  }

  def exception(e: Exception): Unit = {
    logger.error(e.getStackTrace)
  }

}
