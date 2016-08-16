package scheduler

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.FileUtil

/**
  * Created by C.J.YOU on 2016/8/12.
  */
object Scheduler {



   def main(args: Array[String]) {

       /*
         val timeKey = TimeUtil.getTimeKey(-4)
         println("timeKey:" + timeKey)

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

       val sparkConf = new SparkConf().setAppName("QueueStream").setMaster("local")

       // Create the context
       val ssc = new StreamingContext(sparkConf, Seconds(60))
       val sc = ssc.sparkContext
       val data = sc.textFile("F:\\jsdx\\count\\data\\*").map(_.split("\t")).map(separateCount).reduceByKey(_ + _)

       val result = data.collect()

       FileUtil.write("F:\\jsdx\\count\\result\\all_count" , result)



  }

    def separateCount(array: Array[String]) : (String, Int) = {

        val index = array(2).toInt

        if( index >=0 && index <=41)
            ("search",1)
        else if (index >=42 && index <=91)
            ("visit",1)
        else ("None",1)

    }

}
