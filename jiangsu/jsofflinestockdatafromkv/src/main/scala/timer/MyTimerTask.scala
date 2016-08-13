package timer

import java.util.TimerTask
import java.util.concurrent.{ExecutorCompletionService, Executors}

import config.FileConfig
import task.Task
import util.{FileUtil, TimeUtil}

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/8/13.
  */
class MyTimerTask(offSet: Int) extends  TimerTask {


  override def run(): Unit = {

    val timeKey = TimeUtil.getTimeKey(offSet)

    val thread = 24
    val es = Executors.newFixedThreadPool(thread)
    val compService  = new ExecutorCompletionService[ListBuffer[String]](es)

    println("""timer runner start at:"""  + timeKey._1 )

    for(sec <- 0 to 5) {

      val taskBefore = new Task(timeKey._1, sec, 0, 20000, 0)
      val taskLast = new Task(timeKey._1, sec, 20000,50000, 0)

      val taskAfter = new Task(timeKey._1, sec, 0, 20000, 5)
      val taskAfterLast = new Task(timeKey._1, sec, 20000,50000, 5)

      compService.submit(taskBefore)
      compService.submit(taskLast)

      compService.submit(taskAfter)
      compService.submit(taskAfterLast)

    }

    es.shutdown()

    for(sec <- 0 to 5 ) {

      val tempResult = compService.take().get()
      FileUtil.writeToFile(FileConfig.DATA_DIR + "/" + timeKey._2, tempResult)

    }

    println("is over!")

  }
}

object MyTimerTask {

  def apply(offSet: Int): MyTimerTask = new MyTimerTask(offSet)

}
