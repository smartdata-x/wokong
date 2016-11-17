package Test

import java.util.{Calendar, Date, Timer}

import config.{FileConfig, TelecomConfig, XMLConfig}
import log.UserLogger
import timer.MyTimerTask

/**
  * Created by C.J.YOU on 2016/8/13.
  */
object Test {

  val PERIOD_TIME = 60 * 1000


  def main(args: Array[String]) {

    if (args.length < 4) {
      sys.error("Usage args :<xmlFile> <startExecutorTask> <endExecutorTask> <delayTime>")
      sys.exit(-1)
    }

    val Array(xmlFile, startExecutorTask, endExecutorTask, delayTime) = args

    val xmlConfig = XMLConfig.apply(xmlFile)

    FileConfig.DATA_DIR = xmlConfig.DATA_DIR
    FileConfig.LOG_DIR = xmlConfig.LOG_DIR
    FileConfig.PROGRESS_DIR = xmlConfig.PROGRESS_DIR
    UserLogger.logConfigureFile(xmlConfig.LOG_CONFIG)

    TelecomConfig.API_KEY = xmlConfig.API_KEY
    TelecomConfig.USER_NAME = xmlConfig.USER_NAME
    TelecomConfig.PASSWORD = xmlConfig.PASSWORD
    TelecomConfig.TABLE = xmlConfig.TABLE
    TelecomConfig.DATABASE = xmlConfig.DATABASE

    val task = new MyTimerTask(delayTime.toInt, startExecutorTask.toInt, endExecutorTask.toInt)

    task.run()
    val timer = new Timer()
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + 1)
    val startData = cal.getTime
    timer.schedule(task, startData ,PERIOD_TIME)  // 1 Min 之后开始每一分钟跑一次


  }


}
