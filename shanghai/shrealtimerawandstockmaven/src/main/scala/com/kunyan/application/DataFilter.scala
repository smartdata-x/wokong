package com.kunyan.application

import com.kunyan.filter.Filter
import com.kunyan.util.FileUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by C.J.YOU on 2016/12/7.
  */
object DataFilter {


  val sparkConf = new SparkConf().setAppName("DataFilter")

  val sc = new SparkContext(sparkConf)

  def  showWarnings(args: Array[String], length: Int) = {

    if(args.length != length) {
       sys.error("""usage: args parameter length wrong! please check....""")
      sys.exit(-1)
    }

  }

  def main(args: Array[String]) {

    showWarnings(args, 4)

    val Array(dataDir, saveDir, fileName, stockCode) = args

    val data = Filter.filterStockCode(sc, dataDir, stockCode = stockCode)

    FileUtil.filterStockCodeWriter(saveDir + "/" + fileName, data, isAppend = true )

  }

}
