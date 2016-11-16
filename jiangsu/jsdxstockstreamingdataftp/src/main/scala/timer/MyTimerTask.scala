package timer

import java.util.TimerTask

import config.XMLConfig
import log.UserLogger
import task.{RegetTask, Task}
import thread.ThreadPool
import util.{FileUtil, TimeUtil}

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/8/26.
  * 定时器 处理类
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


    UserLogger.info("current time: " + TimeUtil.getTimeKey(0)._1+",timer runner start at:" + timeKey._1)
    FileUtil.writeString(file, "current time: " + TimeUtil.getTimeKey(0)._1+",timer runner start at:" + timeKey._1 )

    for(sec <- startExecutorTask to endExecutorTask) {

      val taskBeforeIn = new Task(timeKey._1, sec, 0, endExecutorTask)
      val taskAfterIn = new Task(timeKey._1, sec, 5, endExecutorTask)
      ThreadPool.COMPLETION_SERVICE.submit(taskBeforeIn)
      ThreadPool.COMPLETION_SERVICE.submit(taskAfterIn)

    }

    // 需要合并文件的集合
    val list = new ListBuffer[(String, String)]

    // 获取文件失败的集合
    val failedFileList = new ListBuffer[String]


    var date = ""
    var hourTime = ""

    for(num <- 0 to totalThread ) {

      val result = ThreadPool.COMPLETION_SERVICE.take().get()

      FileUtil.writeString(file, result + "--------<<<<<<<<<<<-------------------------------")

      // 处理哪些文件需要合并逻辑
      val fileName = result.split(",")(1)
      val time = fileName.replace(XMLConfig.ftpConfig.FILE_PREFIX_NAME,"").replace(XMLConfig.ftpConfig.FILE_SUFFIX_NAME,"")
      hourTime = time.substring(0,10)
      date = time.substring(0,8)

      // 添加failed文件提醒
      if(result.contains("failed"))
        failedFileList.+=(fileName)
      else
        list.+=((fileName,date))


      /*
      // mergefile 来一个合并一个
      val fileName = result.split(",")(1)
      val time = fileName.replace(FTPConfig.FILE_PREFIX_NAME,"").replace(FTPConfig.FILE_SUFFIX_NAME,"")
      val hourTime = time.substring(0,10)
      val date = time.substring(0,8)
      FileUtil.mergeFile(FileConfig.DATA_DIR + "/" + date + "/jsdx_" + hourTime , FileConfig.DATA_DIR + "/" + date + "/temp/" + fileName)
      FileUtil.deleteFile( FileConfig.DATA_DIR + "/" + date + "/" + fileName)
      */

    }

    // reget after 30 s
    if(failedFileList.nonEmpty) {

      UserLogger.info("sleep 30s now for request data.........")
      Thread.sleep(30000)

    }

    for(item <- failedFileList) {

      val fileTime =  item.replace(XMLConfig.ftpConfig.FILE_PREFIX_NAME,"").replace(XMLConfig.ftpConfig.FILE_SUFFIX_NAME,"")
      val regetTask =  new RegetTask(item, fileTime)
      ThreadPool.COMPLETION_SERVICE.submit(regetTask)

    }

    val size = failedFileList.size

    for(index <- 0 until size ) {

      val result = ThreadPool.COMPLETION_SERVICE.take().get()

      FileUtil.writeString(file, result + "--------<<<<<<<<<<<-------------------------------")

      // 处理哪些文件需要合并逻辑
      val fileName = result.split(",")(1)
      val time = fileName.replace(XMLConfig.ftpConfig.FILE_PREFIX_NAME,"").replace(XMLConfig.ftpConfig.FILE_SUFFIX_NAME,"")
      hourTime = time.substring(0,10)
      date = time.substring(0,8)

      // 添加failed文件提醒
      if(!result.contains("failed")) {
        list.+=((fileName,date))
      } else {
        failedFileList.-=(fileName)
      }

    }

    // 正式合并每分钟获取的12个文件
    FileUtil.mergeFile(XMLConfig.ftpConfig.DATA_DIR + "/" + date + "/jsdx_" + hourTime, list.toArray)
    FileUtil.batchFileDelete(list.toArray)

    // 短信提醒
    if(failedFileList.nonEmpty) {

      /*val res = TextSender.send(XMLConfig.ftpConfig.KEY, XMLConfig.ftpConfig.MESSAGE_CONTEXT + ":" + failedFileList.mkString(",") , XMLConfig.ftpConfig.RECEIVER)
      if(res) UserLogger.error("[SUE] MESSAGE SEND SUCCESSFULLY")*/
      UserLogger.info("[warning] MESSAGE: "+ TimeUtil.getTimeKey(0)._1 + ", loss files: " + failedFileList.mkString(","))

    }

    //  变量清空
    failedFileList.clear()
    list.clear()

    // 保存定时进程日志信息
    FileUtil.writeString(file, "current time: "+ TimeUtil.getTimeKey(0)._1 + ", last request is over at: " + timeKey._1 )
    UserLogger.info("current time: "+ TimeUtil.getTimeKey(0)._1 + ", last request is over at: " + timeKey._1)

  }

}

object MyTimerTask {

  // apply
  def apply(offSet: Int, startExecutorTask: Int, endExecutorTask: Int): MyTimerTask = new MyTimerTask(offSet, startExecutorTask, endExecutorTask)

}

