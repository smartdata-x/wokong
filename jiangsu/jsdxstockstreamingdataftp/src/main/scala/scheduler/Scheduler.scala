package scheduler

import java.util.{Calendar, Date, Timer}

import config.XMLConfig
import log.UserLogger
import timer.MyTimerTask

/**
  * Created by C.J.YOU on 2016/8/26.
  * 调度主类
  */
object Scheduler {

  val PERIOD_TIME = 60 * 1000

  def main(args: Array[String]) {

    if(args.length < 4) {
      sys.error(
        """
          |args
          |USAGE: DATA_DIR, LOG_DIR,PROGRESS_DIR,log4j.properties,start,end, delay
          |default: DATA_DIR, LOG_DIR,PROGRESS_DIR,log4j.properties,0,5,-3
          |end
        """.stripMargin)
    }

    val Array(xmlFile, startExecutorTask, endExecutorTask, offSet) = args

   /* val  xmlFile = "E:\\jsdxftpdown.xml"
    val startExecutorTask = "0"
    val endExecutorTask = "5"
    val offSet = "-39"*/

    XMLConfig.apply(xmlFile)

    val conf = XMLConfig.ftpConfig

    UserLogger.info("xml config:"  +  conf.IP + "," +  conf.DATA_DIR + "," +  conf.REMOTE_DIR + ","+ conf.FILE_PREFIX_NAME + "," +  conf.FILE_SUFFIX_NAME)

    val task = MyTimerTask.apply(offSet.toInt, startExecutorTask.toInt, endExecutorTask.toInt)

    task.run()

    val timer = new Timer()
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + 1)
    val startData = cal.getTime
    timer.schedule(task, startData ,PERIOD_TIME)  // 1 Min 之后开始每一分钟跑一次

  }


}
