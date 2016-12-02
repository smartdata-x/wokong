package com.kunyan.wokongsvc.realtimedata.task

import java.sql.DriverManager

import scala.xml.XML

/**
  * Created by sijiansheng on 2016/10/28.
  */
object ReplistFollowAdd {


  val confFile = XML.loadFile("E://heat/config.xml")
  val conn = DriverManager.getConnection((confFile \ "mysql_stock" \ "url").text, (confFile \ "mysql_stock" \ "user").text, (confFile \ "mysql_stock" \ "password").text)

  def main(args: Array[String]) {

    val insertMap = collection.mutable.Map[(String, Long), Int]()
    val newMap = getStockTimeCount

    for (cell <- newMap) {

      val code = cell._1._1
      val time = cell._1._2
      val lastTime = time - 3600

      if (newMap.get((code, lastTime)).isDefined) {
        insertMap += Tuple2((code, time), cell._2 - newMap((code, lastTime)))
      }

    }

    insertAdd(newMap)

    conn.close()
  }

  def getStockTimeCount: Map[(String, Long), Int] = {

    val statement = conn.createStatement()
    val sql = "select * from stock_follow"
    val result = statement.executeQuery(sql)

    val resultMap = collection.mutable.Map[(String, Long), Int]()

    while (result.next()) {
      val stockCode = result.getString(1)
      val timeTamp = result.getLong(2)
      val count = result.getInt(3)
      resultMap += Tuple2((stockCode, timeTamp), count)
    }

    resultMap.toMap
  }

  def insertAdd(codeTimeCount: Map[(String, Long), Int]): Unit = {

    val statement = conn.createStatement()

    for (cell <- codeTimeCount) {

      val code = cell._1._1
      val time = cell._1._2
      val count = cell._2

      val sql = "insert into stock_follow_add set stock_code = \"" + code + "\",timeTamp = \"" + time + "\",count = \"" + count + "\""

      try {
        statement.executeUpdate(sql)
      } catch {
        case e: Exception =>
      }

    }
  }
}
