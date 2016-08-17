package scheduler

import util.TimeUtil

/**
  * Created by C.J.YOU on 2016/8/12.
  */
object Scheduler {


  // def main(args: Array[String]) {


    val timeKey = TimeUtil.getTimeKey(-4)
    println("timeKey:" + timeKey)
/*
    val sparkConf = new SparkConf().setAppName("QueueStream").setMaster("local")

    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    val rddQueue = new mutable.Queue[RDD[Int]]()

    // Create the QueueInputDStream and use it do some processing
    val inputStream = ssc.queueStream(rddQueue)

    // 对 x % 10 的进行计数
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)

    reducedStream.print()
    ssc.start()*/



//  }

}
