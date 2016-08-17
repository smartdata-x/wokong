package task

import java.util.concurrent.{Callable, Executors}

import config.FileConfig
import util.FileUtil

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

/**
  * Created by C.J.YOU on 2016/8/12.
  * key 为分钟级别细分两部分（0 与 5）:min 控制每个时间的段的min
  */
class Task (key: String, second: Int, start:Int, end: Int, last:Int) extends Callable[ListBuffer[String]] {

  override def call(): ListBuffer[String] = {

    val requestKey = key +  (second * 10 + last )

    val listBuffer = new ListBuffer[String]

    val threadInfo = "name: "+ Thread.currentThread().getName +",id:"+ Thread.currentThread().getId

    val thread = 10
    val es = Executors.newFixedThreadPool(thread)

    val break = new Breaks

    break.breakable {

      for (index <- start until end) {

        val subTask = new SubTask(requestKey + "_ky_" + index)
        val value = es.submit(subTask).get()
        if(value.isEmpty) {
          FileUtil.writeString(FileConfig.LOG_DIR +"/" + requestKey.substring(0,10), "null value "+ last + ":" + requestKey + "_ky_" + index + "---" + threadInfo)
          break.break()
        }
        else
          listBuffer.+=(value)

      }

    }

    es.shutdown()

   listBuffer

  }
}
