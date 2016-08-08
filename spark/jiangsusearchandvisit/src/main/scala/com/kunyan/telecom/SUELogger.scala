package com.kunyan.telecom

import org.apache.log4j.Logger

/**
  * Created by C.J.YOU on 2016/8/5.
  */
object SUELogger extends Serializable{

  private  val logger = Logger.getLogger("SUELogger")

  def debug(msg: String): Unit = {
    logger.debug(msg)
  }

  def info(msg: String): Unit = {
    logger.info(msg)
  }

  def warn(msg: String): Unit = {
    logger.warn(msg)
  }

  def error(msg: String): Unit = {
    logger.error(msg)
  }

  def exception(e: Exception): Unit = {
    logger.error(e.getStackTrace)
  }

}
