package logger

import org.apache.log4j.{BasicConfigurator, Logger}

/**
  * Created by sijiansheng on 2016/11/16.
  */
object HeatLogger {

  val logger = Logger.getLogger("LoggerTest")
  BasicConfigurator.configure()

  def exception[U <: Throwable](e: U) = {
    logger.error(e.getLocalizedMessage)
    logger.error(e)
    logger.error(e.getStackTraceString)
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
