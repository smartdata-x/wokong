package log

import org.apache.log4j.Logger

/**
  * Created by C.J.YOU on 2016/8/13.
  * 日志类
  */
object UserLogger extends Serializable {

  private  val LOGGER = Logger.getLogger("Logger")

  def debug(msg: String): Unit = {
    LOGGER.debug(msg)
  }

  def info(msg: String): Unit = {
    LOGGER.info(msg)
  }

  def warn(msg: String): Unit = {
    LOGGER.warn(msg)
  }

  def error(msg: String): Unit = {
    LOGGER.error(msg)
  }

  def exception(e: Exception): Unit = {
    LOGGER.error(e.getStackTrace)
  }

}
