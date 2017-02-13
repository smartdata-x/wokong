package com.kunyan.wokongsvc.realtimedata.task.comparison

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by sijiansheng on 2017/1/5.
  */
object MonthStatics {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("StockComparison").setMaster("local[4]")
    val stc = new SparkContext(sparkConf)
//    val stockType = "visit"
    val orderFile = args(1)

    val result = stc.textFile(args(0)).map(line => {
      val cells = line.split(" ")
      val host = cells(1)
      val dataType = cells(5)
      val normalLevel = cells(6)
      val count = cells(7).toLong
      (s"$host $dataType $normalLevel", count)
    }).reduceByKey(_ + _).collect()

    System.currentTimeMillis()

    DayStatics.saveAsCSVWithSTC(result, s"$orderFile.txt")
  }
}
