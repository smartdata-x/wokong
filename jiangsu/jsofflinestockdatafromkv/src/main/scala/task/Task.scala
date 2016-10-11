package task

import java.util.concurrent.Callable

import config.FileConfig
import thread.ThreadPool
import util.FileUtil

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

/**
  * Created by C.J.YOU on 2016/8/12.
  * key 为分钟级别细分两部分（0 与 5）: second 用来对秒取整
  * 多个请求的线程处理类
  */
class Task (key: String, second: Int, start:Int, end: Int, last:Int, taskId: Int) extends Callable[(ListBuffer[String],Int)] {

  override def call(): (ListBuffer[String],Int) = {

    val sec = second * 10 + last
    val requestKey = key +  sec

    val listBuffer = new ListBuffer[String]

    val threadInfo = "name: "+ Thread.currentThread().getName +",id: "+ Thread.currentThread().getId

    val break = new Breaks

    var max = start

    val threshold = 20

    var count = 0

    val dir = FileConfig.LOG_DIR + "/" + requestKey.substring(0,8)

    val taskIdDir = dir + "/" + taskId

    val file = taskIdDir + "/" + requestKey.substring(0,10) + "__" + taskId  // + "__" + sec

    FileUtil.mkDir(dir)
    FileUtil.mkDir(taskIdDir)

    // 每秒日志数据分割-----
    break.breakable {

      for (index <- start until end) {

        val subTask = new SubTask(requestKey + "_ky_" + index)

        val value = ThreadPool.THREAD_EXECUTOR_SERVICE.submit(subTask).get()

        if(value.isEmpty) {

          count = count + 1

          if(count > threshold) {

            FileUtil.writeString(file, key + " __log__ " + sec + " >>>>>>>>>>>-----------------------")
            FileUtil.writeString(file, "key from: " + (index - threshold) + " to " + index + " is null,set threshold: " + threshold)
            break.break()

          }
        } else {

          count = 0
          max = index
          listBuffer.+=(value)

        }

      }

    }

    if(max != start)
      FileUtil.writeString(file, "is null value at "+ sec + " : " + requestKey.substring(0,12) + "_ky_ max index is less than :" + max + "---" + threadInfo + " <<<<<<<<<<<-------------------------------")

    (listBuffer,max - start + 1)

  }
}