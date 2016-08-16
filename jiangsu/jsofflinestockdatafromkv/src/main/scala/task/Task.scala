package task

import java.util.concurrent.{Callable, ExecutorCompletionService, Executors}

import config.FileConfig
import util.FileUtil

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

/**
  * Created by C.J.YOU on 2016/8/12.
  * key 为分钟级别细分两部分（0 与 5）: second 用来对秒取整
  * 多个请求的线程处理类
  */
class Task (key: String, second: Int, start:Int, end: Int, last:Int) extends Callable[ListBuffer[String]] {

  override def call(): ListBuffer[String] = {

    // val sec = second * 10 + last
    val requestKey = key // +  sec

    val listBuffer = new ListBuffer[String]

    val threadInfo = "name: "+ Thread.currentThread().getName +",id:"+ Thread.currentThread().getId

    val thread = 50
    val es = Executors.newFixedThreadPool(thread)
    val compService  = new ExecutorCompletionService[String](es)
    val break = new Breaks

    var max = start

    break.breakable {

      for (index <- start until end) {

        val subTask = new SubTask(requestKey + "_ky_" + index)
        // compService.submit(subTask)
        val value = es.submit(subTask).get()

        if(value.isEmpty) {
          // FileUtil.writeString(FileConfig.LOG_DIR +"/" + requestKey.substring(0,10), "null value "+ last + ":" + requestKey + "_ky_" + index + "---" + threadInfo)
          break.break()
        }
        else {

          max = index
          listBuffer.+=(value)

        }

      }

    }
    /*for(index <- start until end) {
      val value = compService.take().get()
      if(value.isEmpty) {
        // FileUtil.writeString(FileConfig.LOG_DIR +"/" + requestKey.substring(0,10), "null value "+ last + ":" + requestKey + "_ky_" + index + "---" + threadInfo)
        // break.break()
      }else {
        max = index
        listBuffer.+=(value)
      }
    }*/

    if(max != start)
      FileUtil.writeString(FileConfig.LOG_DIR +"/" + requestKey.substring(0,10) + "_" + sec, "is null value at "+ sec + ":" + requestKey + "_ky_ max index is less than :" + max + "---" + threadInfo)

    es.shutdown()

   listBuffer

  }
}
