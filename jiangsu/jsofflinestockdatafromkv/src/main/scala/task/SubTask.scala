package task

import java.util.concurrent.Callable

import request.Request

/**
  * Created by C.J.YOU on 2016/8/13.
  * 单个请求的子线程处理类
  */
class SubTask(key: String) extends Callable[String] {

  override def call(): String = {

    val threadInfo = "name: "+ Thread.currentThread().getName +",id:"+ Thread.currentThread().getId

    val value = Request.getValue(key)

    value

  }
}