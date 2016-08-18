package thread

import java.util.concurrent.{ExecutorCompletionService, Executors}

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/8/16.
  */
object ThreadPool {

  val timerThreadNum = 150

  val timeThreadExecutorService = Executors.newFixedThreadPool(timerThreadNum)

  val compService  = new ExecutorCompletionService[ListBuffer[String]](timeThreadExecutorService)

}