package timer

import java.util.TimerTask
import java.util.concurrent.{ExecutorCompletionService, Executors}

import config.FileConfig
import task.Task
import util.{FileUtil, TimeUtil}

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/8/13.
  * 定时开始请求的Task类，定时一分钟
  */
class MyTimerTask(offSet: Int) extends  TimerTask {


  override def run(): Unit = {

    val timeKey = TimeUtil.getTimeKey(offSet)

    val MAX_REQUEST = 3000

    val thread = 130
    val es = Executors.newFixedThreadPool(thread)
    val compService  = new ExecutorCompletionService[ListBuffer[String]](es)

    FileUtil.writeString(FileConfig.PROGRESS_DIR +"/" + timeKey._2, "current time: " + TimeUtil.getTimeKey(0)._1+",timer runner start at:" + timeKey._1 )
    println("current time: " + TimeUtil.getTimeKey(0)._1+",timer runner start at:" + timeKey._1 )

    for(sec <- 0 to 5) {

      for(num <- 0 to 9) {

        val taskBeforeIn = new Task(timeKey._1, sec, num * MAX_REQUEST, (num + 1) * MAX_REQUEST, -1)
        val taskAfterIn = new Task(timeKey._1, sec, num * MAX_REQUEST, (num + 1) * MAX_REQUEST, -1)
        compService.submit(taskBeforeIn)
        compService.submit(taskAfterIn)

      }

    }

    es.shutdown()

    for(sec <- 0 to 119) {

      val tempResult = compService.take().get()

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
