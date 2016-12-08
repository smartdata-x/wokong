package com.kunyan.test

import com.kunyan.util.{FileUtil, TimeUtil}

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by C.J.YOU on 2016/12/7.
  */
object TestDataFilter {

  def main(args: Array[String]) {

    val resultLB = new ListBuffer[String]

    val data = Source.fromFile("").getLines().foreach{ line =>

      val dataSplit = line.split("\t")
      val timeStamp = dataSplit(0)
      val transTime = TimeUtil.formatTimeStamp(timeStamp)
      // println(ctime)
      val result = transTime + "\t" + dataSplit.slice(1,dataSplit.length).mkString("\t")

      resultLB.+=(result)

    }

    FileUtil.normalWriter("", resultLB.toArray, isAppend = true)

  }

}
