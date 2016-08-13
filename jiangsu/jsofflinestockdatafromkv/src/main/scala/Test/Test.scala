package Test

import java.util.{Date, Timer}

import timer.MyTimerTask

/**
  * Created by C.J.YOU on 2016/8/13.
  */
object Test {



  val PERIOD_TIME = 60 * 1000


  def main(args: Array[String]) {

    val task = new MyTimerTask(-31)
    task.run()
    val timer = new Timer()
    timer.schedule(task, new Date(),PERIOD_TIME)

  }



}
