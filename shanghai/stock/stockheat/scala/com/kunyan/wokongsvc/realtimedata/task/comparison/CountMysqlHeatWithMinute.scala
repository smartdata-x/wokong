package com.kunyan.wokongsvc.realtimedata.task.comparison

import java.io.{FileWriter, File}
import java.sql.DriverManager
import java.text.SimpleDateFormat

import com.kunyan.wokongsvc.realtimedata.TimeHandle

import scala.xml.XML

/**
  * Created by sijiansheng on 2017/1/13.
  */
object CountMysqlHeatWithMinute {

  def main(args: Array[String]) {

    val xml = XML.loadFile(args(0))
    val conn = DriverManager.getConnection((xml \ "mysql_stock" \ "url").text, (xml \ "mysql_stock" \ "user").text, (xml \ "mysql_stock" \ "password").text)
    val statement = conn.createStatement()
    val startTime = args(1)
    val endTime = args(2)
    val resultPath = args(3)
    val startTamp = TimeHandle.timeTamp(startTime, "yyyy-MM-dd-HH-mm") / 1000
    val endTamp = TimeHandle.timeTamp(endTime, "yyyy-MM-dd-HH-mm") / 1000
    val mysqlData = statement.executeQuery(s"select timeTamp,count from stock_visit_old where timeTamp <= $endTamp and timeTamp >= $startTamp")

    val mysqlResultMap = collection.mutable.HashMap[Long, Int]()

    while (mysqlData.next()) {

      val mysqlTamp = mysqlData.getLong(1)
      val mysqlCount = mysqlData.getInt(2)
      if (!mysqlResultMap.contains(mysqlTamp)) {
        mysqlResultMap.put(mysqlTamp, mysqlCount)
      } else {
        mysqlResultMap.put(mysqlTamp, mysqlResultMap(mysqlTamp) + mysqlCount)
      }
    }

    statement.close()
    conn.close()

    val i = ((endTamp - startTamp) / 60).toInt

    val file = new File(resultPath)
    val writer = new FileWriter(resultPath, false)

    for (x <- 0 to i) {
      val tempTamp = startTamp + x * 60
      val timeString = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(tempTamp * 1000)
      if (mysqlResultMap.contains(tempTamp)) {
        writer.write(s"$timeString ${mysqlResultMap(tempTamp)}\n")
      } else {
        writer.write(s"$timeString 0\n")
      }
    }

    writer.close()

  }

}
