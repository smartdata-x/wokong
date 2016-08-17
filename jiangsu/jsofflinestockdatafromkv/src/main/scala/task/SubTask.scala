package task

import java.util.concurrent.Callable

import request.Request

/**
  * Created by C.J.YOU on 2016/8/13.
  */
class SubTask(key: String) extends Callable[String] {

  override def call(): String = {

    val threadInfo = "anme: "+ Thread.currentThread().getName +",id:"+ Thread.currentThread().getId

    // FileUtil.writeString(FileConfig.LOG_DIR +"/subTask_" + key.substring(0,10), "Request:  "+ key +",  using threadInfo ---" + threadInfo)

    val value = Request.getValue(key)
    value

  }
}
