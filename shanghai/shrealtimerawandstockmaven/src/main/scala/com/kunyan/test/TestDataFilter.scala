package com.kunyan.test

import com.kunyan.util.FileUtil

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by C.J.YOU on 2016/12/7.
  */
object TestDataFilter {

  def main(args: Array[String]) {

    val resultLB = new ListBuffer[String]

    val path = "F:\\datatest\\telecom\\WOKONG\\上海数据统计文件\\"

    val data = Source.fromFile(path + "all_stock.csv").getLines().foreach{ line =>

      val stock = line.split(",")(0).replace("\"","")

      resultLB.+=(stock)

    }

    FileUtil.normalWriter(path + "stock", resultLB.toArray, isAppend = true)

  }

}
