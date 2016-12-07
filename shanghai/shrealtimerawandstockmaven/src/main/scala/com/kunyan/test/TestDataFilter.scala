package com.kunyan.test

import java.text.SimpleDateFormat
import java.util.Date

import com.kunyan.util.FileUtil

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by C.J.YOU on 2016/12/7.
  */
object TestDataFilter {

  def main(args: Array[String]) {

    val format = new SimpleDateFormat("yyyy-MM-dd_HHmm")

    val resultLB = new ListBuffer[String]

    val data = Source.fromFile("").getLines().foreach{ line =>

      val dataSplit = line.split("\t")
      val timeStamp = dataSplit(0).toLong
      val date = new Date(timeStamp)
      val transTime = format.format(date)
      // println(ctime)
      val result = transTime + "\t" + dataSplit.slice(1,dataSplit.length).mkString("\t")

      resultLB.+=(result)

    }

    FileUtil.writeToFile("", resultLB, true)

  }

}
