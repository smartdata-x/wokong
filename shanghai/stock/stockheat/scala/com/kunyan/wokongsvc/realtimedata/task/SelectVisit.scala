package com.kunyan.wokongsvc.realtimedata.task

import java.io.{File, PrintWriter}
import java.sql
import java.sql.DriverManager
import java.text.SimpleDateFormat

import scala.xml.{Elem, XML}


/**
  * Created by sijiansheng on 2016/11/22.
  */
object SelectVisit {

  var mysql: sql.Connection = _

  def main(args: Array[String]) {

    val confFile = XML.loadFile("E://heat/config.xml")
    initMysql(confFile)
    val statement = mysql.createStatement()
    val sql = "select timeTamp,count from stock_visit_old where stock_code = \"000651\" and timeTamp > 1479916800 and timeTamp < 1480003200"
    val result = statement.executeQuery(sql)
    val needInsertData = collection.mutable.Map[String, Int]()

    while (result.next()) {

      val timeTamp = result.getLong(1)
      val timeString = getTimeStringByTimeTamp(timeTamp * 1000)
      val sum = result.getInt(2)
      println(s"$timeString:$sum")
      needInsertData.put(timeString, sum)

    }

    mysql.close()

    val writer = new PrintWriter(new File("E://000651-20161124.txt"), "utf-8")
    needInsertData.foreach(timeAndSum => {
      writer.write(timeAndSum._1 + "  " + timeAndSum._2 + "\n")
    })

    writer.close()
  }

  def initMysql(confFile: Elem): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    this.mysql = DriverManager.getConnection((confFile \ "mysql_stock" \ "url").text, (confFile \ "mysql_stock" \ "user").text, (confFile \ "mysql_stock" \ "password").text)
  }

  def getTimeStringByTimeTamp(timeTamp: Long): String = {
    new SimpleDateFormat("yyyyMMddHHmm").format(timeTamp)
  }
}
