package timer

import java.util.TimerTask

import config.FileConfig
import task.Task
import thread.ThreadPool
import util.{FileUtil, TimeUtil}

/**
  * Created by C.J.YOU on 2016/8/13.
  * 定时开始请求的Task类，定时一分钟
  */
class MyTimerTask(offSet: Int) extends  TimerTask {


  override def run(): Unit = {

    val timeKey = TimeUtil.getTimeKey(offSet)

    val MAX_REQUEST = 3000

    FileUtil.writeString(FileConfig.PROGRESS_DIR +"/" + timeKey._2, "current time: " + TimeUtil.getTimeKey(0)._1+",timer runner start at:" + timeKey._1 )
    println("current time: " + TimeUtil.getTimeKey(0)._1+",timer runner start at:" + timeKey._1 )

    for(min <- 0 to 59) {

      for(num <- 0 to 9) {

        val taskBeforeIn = new Task(timeKey._1, min, num * MAX_REQUEST, (num + 1) * MAX_REQUEST, -1)
        ThreadPool.compService.submit(taskBeforeIn)

      }
    }

    for(sec <- 0 to 599) {

      val tempResult = ThreadPool.compService.take().get()

      if(tempResult.nonEmpty) {
        println("size:" + tempResult.size)
      }
      FileUtil.writeToFile(FileConfig.DATA_DIR + "/" + timeKey._2, tempResult)

    }

    println("is over")
    FileUtil.writeString(FileConfig.PROGRESS_DIR +"/" + timeKey._2, "current time: "+ TimeUtil.getTimeKey(0)._1 + ", last request is over at: " + timeKey._1)

  }
}

object MyTimerTask {

  def apply(offSet: Int): MyTimerTask = new MyTimerTask(offSet)

}
