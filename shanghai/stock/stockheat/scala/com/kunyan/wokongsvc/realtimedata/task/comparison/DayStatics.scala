package com.kunyan.wokongsvc.realtimedata.task.comparison

import java.io.{FileWriter, File}
import java.text.SimpleDateFormat

import com.kunyan.wokongsvc.realtimedata
import com.kunyan.wokongsvc.realtimedata.DataPattern._
import com.kunyan.wokongsvc.realtimedata.{DataPattern, MysqlHandle, XmlHandle, FileHandle}
import com.kunyan.wokongsvc.realtimedata.task.ReplenshVisit
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer

/**
  * Created by sijiansheng on 2017/1/5.
  */
object DayStatics {

  val INTERVAL = 1000 * 60 * 60 * 12L

  def main(args: Array[String]) {

    val flag = args(0)
    val message = args(1)
    /* 初始化xml配置 */
    val xml = XmlHandle(args(2))
    /* 初始化mysql操作句柄 */
    val sqlHandle = MysqlHandle(xml.getElem("mysql_stock", "url"), xml)
    val stockalias = ReplenshVisit.getStockAlias(sqlHandle)
    val sparkConf = new SparkConf().setAppName("StockComparison").setMaster("local")
    val stc = new SparkContext(sparkConf)

    flag match {

      case "file" => executeByFile(message, stc, stockalias)
      case "time" => executeByTime(message, stc, stockalias)
      case _ =>
        realtimedata.logger.error("文件标志错误，文件格式为file或者time")
        System.exit(-1)

    }

    stc.stop()
  }

  def executeByTime(message: String, sc: SparkContext, alias: Tuple2Map): Unit = {

    val messageArray = message.split(",")
    val times = messageArray(0).split(":")
    val timeIntervals = getTimeByStartAndEnd(times(0), times(1))
    val orderFilePrefix = messageArray(1)
    val resultFilePrefix = messageArray(2)
    val stockType = messageArray(3)

    timeIntervals.foreach(timeString => executeByType(orderFilePrefix + timeString, sc, alias, resultFilePrefix, stockType, timeString))
  }

  def getTimeByStartAndEnd(startTime: String, endTime: String): Array[String] = {

    val startTamp = new SimpleDateFormat("yyyy-MM-dd").parse(startTime).getTime
    val endTamp = new SimpleDateFormat("yyyy-MM-dd").parse(endTime).getTime
    val intervals = ((endTamp - startTamp) / INTERVAL).toInt
    val result = new ListBuffer[String]()
    for (i <- 0 to intervals) {
      result += new SimpleDateFormat("yyyy-MM-dd").format(startTamp + (INTERVAL * i))
    }

    result.toArray
  }

  def executeByFile(message: String, sc: SparkContext, alias: Tuple2Map): Unit = {

    val messageArray = message.split(",")
    val nameFilePath = messageArray(0)
    val orderFilePrefix = messageArray(1)
    val resultFilePrefix = messageArray(2)
    val stockType = messageArray(3)


    var fileSuffix: String = null

    while ( {
      fileSuffix = readFile(nameFilePath)
      fileSuffix
    } != null) {

      val filePath = orderFilePrefix + fileSuffix
      executeByType(filePath, sc, alias, resultFilePrefix, stockType, fileSuffix)
    }

  }

  def executeByType(filePath: String, sc: SparkContext, alias: Tuple2Map, resultFilePrefix: String, stockType: String, resultFileSuffix: String): Unit = {


    stockType match {

      case "visit" =>
      case "search" =>
      case "all" => executeStatistics(filePath: String, sc: SparkContext, alias, resultFilePrefix, resultFileSuffix)
      case _ =>
        realtimedata.logger.error("统计类型错误，请选择visit,search，all")
        System.exit(-1)

    }
  }

  def executeStatistics(filePath: String, sc: SparkContext, alias: Tuple2Map, resultFilePath: String, resultFileSuffix: String): Unit = {

    val result = sc.textFile(filePath).map(line => handleLine(line, alias, statistict)).reduceByKey(_ + _).collect() //todo reducebykey如何实现不区分int long或者其他类型的
    saveAsCSVWithSTC(result, s"$resultFilePath$resultFileSuffix.txt")
  }

  def saveAsCSVWithSTC(rdd: Array[(String, Long)], path: String) {

    val file = new File(path)
    val writer = new FileWriter(path, false)
    rdd.foreach { cell =>
      writer.write(s"${cell._1} ${cell._2}\n")
    }
    writer.close()
  }

  def handleLine(line: String, alias: Tuple2Map, parserMethod: (String, Tuple2Map) => (String, Long)): (String, Long) = {
    parserMethod(line, alias)
  }


  def statistict(line: String, alias: Tuple2Map): (String, Long) = {

    val cells = line.split("\\t")

    if (cells.length < 10) {
      return ("0", 0)
    }

    val tsMin = cells(0)
    val ad = cells(1)
    val stock = cells(2)
    val ts = cells(3)
    val stockType = cells(4).toInt
    val levle = cells(5).toInt
    val count = cells(6)
    val ua = cells(7)
    val host = cells(8)
    val url = cells(9)
    var levelFlag = "normal"
    var typeFlag = ""

    val mappedType = {

      val stockCode = DataPattern.stockCodeMatch(stock, alias)

      if (stockType >= 0 && stockType <= 42) {
        typeFlag = "search"
      } else if (stockType >= 43 && stockType <= 91) {
        typeFlag = "visit"
      } else {
        return ("0", 0)
      }

      if (levle > 2) levelFlag = "unnormal"

      (s"$ad $host $url $stock $tsMin $typeFlag $levelFlag", 1L)
    }

    mappedType
  }

  def readFile(path: String): String = {

    val fileHandle = FileHandle(path)
    val reader = fileHandle.initBuff()
    var (delete, ident, save) = (reader.readLine(), reader.readLine(), "")

    while (ident != null) {
      save += ident + "\n"
      ident = reader.readLine()
    }

    reader.close()

    val writer = fileHandle.initWriter()
    writer.write(save.trim)
    writer.close()
    println(s"删除文件中的：$delete")
    delete
  }

  //  def obtainOrderFileByFile()
}
