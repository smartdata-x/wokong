package thread

import java.util.concurrent.{ExecutorCompletionService, Executors}

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/8/16.
  * 线程池的维护
  */
object ThreadPool {

  val THREAD_NUMBER = 150

  val THREAD_EXECUTOR_SERVICE = Executors.newFixedThreadPool(THREAD_NUMBER)

  val COMPLETION_SERVICE  = new ExecutorCompletionService[ListBuffer[String]](THREAD_EXECUTOR_SERVICE)

}