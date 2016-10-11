package timer

import java.util.TimerTask

import config.FileConfig
import task.Task
import thread.ThreadPool
import util.{FileUtil, TimeUtil}

/**
  * Created by C.J.YOU on 2016/8/13.
  * 定时开始请求的Task类
  * @param offSet 延迟的时间（MIN）
  * @param startExecutorTask  start 决定请求index的范围区间 一个区间长度为 MAX_REQUEST = 3000
  * @param endExecutorTask end
  */
class MyTimerTask(offSet: Int, startExecutorTask: Int, endExecutorTask: Int) extends TimerTask {

  val totalThread = 6 * (endExecutorTask - startExecutorTask + 1) * 2 - 1

  override def run(): Unit = {

    val timeKey = TimeUtil.getTimeKey(offSet)

    val MAX_REQUEST = 3000

    val dir = FileConfig.PROGRESS_DIR + "/" + timeKey._2.substring(0,8)
    FileUtil.mkDir(dir)

    val taskIdDir = dir + "/" + endExecutorTask

    FileUtil.mkDir(taskIdDir)

    val file = taskIdDir + "/" + timeKey._2  + "_" + endExecutorTask


    println("current time: " + TimeUtil.getTimeKey(0)._1+",timer runner start at:" + timeKey._1)
    FileUtil.writeString(file, "current time: " + TimeUtil.getTimeKey(0)._1+",timer runner start at:" + timeKey._1 )

    for(sec <- 0 to 5) {

      for(num <- startExecutorTask to endExecutorTask) {

        val taskBeforeIn = new Task(timeKey._1, sec, num * MAX_REQUEST, (num + 1) * MAX_REQUEST, 0, endExecutorTask)
        val taskAfterIn = new Task(timeKey._1, sec, num * MAX_REQUEST, (num + 1) * MAX_REQUEST, 5, endExecutorTask)
        ThreadPool.COMPLETION_SERVICE.submit(taskBeforeIn)
        ThreadPool.COMPLETION_SERVICE.submit(taskAfterIn)

      }

    }

    println("total:" + totalThread)

    var count = 0
    var countArraySize =0

    for(sec <- 0 to totalThread) {

      val future = ThreadPool.COMPLETION_SERVICE.take().get()

      val tempResult = future._1

      val size = future._2

      count += size

      countArraySize += tempResult.length

      FileUtil.write(FileConfig.DATA_DIR + "/" + timeKey._2 + "_" + endExecutorTask, tempResult.toArray)


    }

    FileUtil.writeString(file, "current time: "+ TimeUtil.getTimeKey(0)._1 + ", last request is over at: " + timeKey._1)
    FileUtil.writeString(file, "data count last request in :"+ timeKey._1 + ",size : " + count +",arraysize:" + countArraySize)
    println("current time: "+ TimeUtil.getTimeKey(0)._1 + ", last request is over at: " + timeKey._1)

  }
}


/**
  * 伴生对象
  */
object MyTimerTask {

  def apply(offSet: Int, startExecutorTask: Int, endExecutorTask: Int): MyTimerTask = new MyTimerTask(offSet, startExecutorTask, endExecutorTask)

}
