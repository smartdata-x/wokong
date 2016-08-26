package timer

import java.util.TimerTask

import config.XMLConfig
import task.Task
import thread.ThreadPool
import util.{FileUtil, TimeUtil}

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/8/26.
  */
class MyTimerTask(offSet: Int,startExecutorTask: Int, endExecutorTask: Int) extends TimerTask {

  val totalThread = (endExecutorTask - startExecutorTask + 1) * 2 - 1

  override def run(): Unit = {

    val timeKey = TimeUtil.getTimeKey(offSet)

    val MAX_REQUEST = 3000

    val dir = XMLConfig.ftpConfig.PROGRESS_DIR + "/" + timeKey._2.substring(0,8)
    FileUtil.mkDir(dir)

    val taskIdDir = dir + "/" + endExecutorTask

    FileUtil.mkDir(taskIdDir)

    val file = taskIdDir + "/" + timeKey._2  + "_" + endExecutorTask


    println("current time: " + TimeUtil.getTimeKey(0)._1+",timer runner start at:" + timeKey._1)
    FileUtil.writeString(file, "current time: " + TimeUtil.getTimeKey(0)._1+",timer runner start at:" + timeKey._1 )

    for(sec <- startExecutorTask to endExecutorTask) {

      val taskBeforeIn = new Task(timeKey._1, sec, 0, endExecutorTask)
      val taskAfterIn = new Task(timeKey._1, sec, 5, endExecutorTask)
      ThreadPool.COMPLETION_SERVICE.submit(taskBeforeIn)
      ThreadPool.COMPLETION_SERVICE.submit(taskAfterIn)

    }

    val list = new ListBuffer[(String, String)]


    var date = ""
    var hourTime = ""

    for(num <- 0 to totalThread ) {

      val result = ThreadPool.COMPLETION_SERVICE.take().get()
      // println("result:" + result)
      FileUtil.writeString(file, result + "--------<<<<<<<<<<<-------------------------------")

      val fileName = result.split(",")(1)
      val time = fileName.replace(XMLConfig.ftpConfig.FILE_PREFIX_NAME,"").replace(XMLConfig.ftpConfig.FILE_SUFFIX_NAME,"")
      hourTime = time.substring(0,10)
      date = time.substring(0,8)
      list.+=((fileName,date))

      // mergefile
      /*val fileName = result.split(",")(1)
      val time = fileName.replace(FTPConfig.FILE_PREFIX_NAME,"").replace(FTPConfig.FILE_SUFFIX_NAME,"")
      val hourTime = time.substring(0,10)
      val date = time.substring(0,8)
      FileUtil.mergeFile(FileConfig.DATA_DIR + "/" + date + "/jsdx_" + hourTime , FileConfig.DATA_DIR + "/" + date + "/temp/" + fileName)
      FileUtil.deleteFile( FileConfig.DATA_DIR + "/" + date + "/" + fileName)*/

    }

    FileUtil.mergeFile(XMLConfig.ftpConfig.DATA_DIR + "/" + date + "/jsdx_" + hourTime, list.toArray)
    FileUtil.batchFileDelete(list.toArray)

    // println("list:" + list)

    list.clear()


    FileUtil.writeString(file, "current time: "+ TimeUtil.getTimeKey(0)._1 + ", last request is over at: " + timeKey._1 )
    println("current time: "+ TimeUtil.getTimeKey(0)._1 + ", last request is over at: " + timeKey._1)

  }

}

object MyTimerTask {

  def apply(offSet: Int, startExecutorTask: Int, endExecutorTask: Int): MyTimerTask = new MyTimerTask(offSet, startExecutorTask, endExecutorTask)

}

