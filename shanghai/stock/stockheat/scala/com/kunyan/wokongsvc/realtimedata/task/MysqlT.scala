package com.kunyan.wokongsvc.realtimedata.task

import java.sql
import java.sql.DriverManager

import com.mysql.jdbc.Connection

import scala.xml.{XML, Elem}


/**
  * Created by sijiansheng on 2016/11/22.
  */
object MysqlT {

  var mysql: sql.Connection = _

  def main(args: Array[String]) {

    val confFile = XML.loadFile("E://heat/config.xml")
    initMysql(confFile)
    val statement = mysql.createStatement()
    val sql = "select stock_code,sum(count) as sum from stock_visit group by stock_code"
    val result = statement.executeQuery(sql)
    val needInsertData = collection.mutable.Map[String, Int]()

    while (result.next()) {
      val stockCode = result.getString(1)
      val sum = result.getInt(2)
      needInsertData.put(stockCode, sum)
    }

    println(needInsertData.size)

    needInsertData.foreach(data => {
      statement.addBatch("update stock_visit_month_11 set day_22 = " + data._2 + " where stock_code = " + data._1)
    })

    statement.executeBatch()

    mysql.close()
  }

  def initMysql(confFile: Elem): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    this.mysql = DriverManager.getConnection((confFile \ "mysql_stock" \ "url").text, (confFile \ "mysql_stock" \ "user").text, (confFile \ "mysql_stock" \ "password").text)
  }

}
