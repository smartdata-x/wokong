package log

import org.apache.log4j.Logger

/**
  * Created by yangshuai on 2016/1/15.
  */
object PrismLogger {

  private  val logger = Logger.getRootLogger

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
