package thread

import java.util.concurrent.{ExecutorCompletionService, Executors}

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/8/16.
  */
object ThreadPool {

  val thread = 150
  val es = Executors.newFixedThreadPool(thread)
  val compService  = new ExecutorCompletionService[ListBuffer[String]](es)

}
