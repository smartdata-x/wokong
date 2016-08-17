package Test

import java.util.{Calendar, Date, Timer}

import config.FileConfig
import log.UserLogger
import timer.MyTimerTask

/**
  * Created by C.J.YOU on 2016/8/13.
  */
object Test {

  val PERIOD_TIME = 60 * 1000


  def main(args: Array[String]) {

    FileConfig.DATA_DIR = args(0)
    FileConfig.LOG_DIR = args(1)
    FileConfig.PROGRESS_DIR = args(2)
    UserLogger.logConfigureFile(args(3))

    val task = new MyTimerTask(args(4).toInt)

    // val task = new MyTimerTask(-23)
    task.run()
    val timer = new Timer()
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + 1)
    val startData = cal.getTime
    timer.schedule(task, startData ,PERIOD_TIME) // 1 Min 之后开始每一分钟跑一次

  }


}
