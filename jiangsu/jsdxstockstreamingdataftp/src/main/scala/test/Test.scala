package test

import java.util.{Calendar, Date, Timer}

import config.XMLConfig
import timer.MyTimerTask

/**
  * Created by C.J.YOU on 2016/8/26.
  */
object Test {

  val PERIOD_TIME = 60 * 1000

  def main(args: Array[String]) {

    val  xml = XMLConfig.apply("E:\\jsdxftpdown.xml")

    val task = MyTimerTask.apply(-3, 0, 5)
    task.run()

    val timer = new Timer()
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + 1)
    val startData = cal.getTime
    timer.schedule(task, startData ,PERIOD_TIME)  // 1 Min 之后开始每一分钟跑一次

  }

}
