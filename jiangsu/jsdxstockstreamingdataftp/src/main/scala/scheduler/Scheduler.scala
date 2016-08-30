package scheduler

import java.util.{Calendar, Date, Timer}

import config.XMLConfig
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

    XMLConfig.apply(xmlFile)

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
