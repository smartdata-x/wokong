package com.kunyan.wokongsvc.realtimedata.task

import java.io.File
import java.sql.DriverManager

import jxl.Workbook
import jxl.write.{Number, Label}

import scala.collection.mutable
import scala.xml.XML

/**
  * Created by sijiansheng on 2016/10/25.
  */
object CountHeatWithExcel {

  val confFile = XML.loadFile("E://heat/config.xml")
  val mysqlConn = DriverManager.getConnection((confFile \ "mysql" \ "totalurl").text, (confFile \ "mysql" \ "userstock").text, (confFile \ "mysql" \ "passwordstock").text)

  def main(args: Array[String]) {
    createXLS("e://test.xls")
  }

  def createXLS(path: String): Unit = {

    val book = Workbook.createWorkbook(new File(path))
    val sheet = book.createSheet("热度数据", 0)
    val sheet2 = book.createSheet("热度统计折线图", 1)
    val label = new Label(0, 0, "")
    val label1 = new Label(0, 1, "搜索热度统计")
    val label2 = new Label(0, 2, "查看热度统计")
    val label3 = new Label(0, 3, "总热度统计")

    for (i <- 1 to 31) {
      val tempLabel = new Label(i, 0, s"10月${i}日")
      sheet.addCell(tempLabel)
    }

    val month = 10
    sheet.addCell(label)
    sheet.addCell(label1)
    sheet.addCell(label2)
    sheet.addCell(label3)

    val searchHeat = getMysqlData("stock_search_month_" + month, true, month.toString)
    val visitHeat = getMysqlData("stock_visit_month_" + month, true, month.toString)

    val keys = searchHeat.keySet
    var i = 1

    keys.foreach(key => {
      val searchNumber = new Number(i, 1, searchHeat(key))
      val visitNumber = new Number(i, 2, visitHeat(key))
      sheet.addCell(searchNumber)
      sheet.addCell(visitNumber)
      i += 1
    })

    book.write()
    book.close()
  }

  def getMysqlData(tableName: String, needThrityOne: Boolean, month: String): mutable.LinkedHashMap[String, Int] = {

    val statement = mysqlConn.createStatement()

    val resultMap = collection.mutable.LinkedHashMap[String, Int]()
    var mysqlColumns = "stock_code"

    var mapSize = 30

    if (needThrityOne) mapSize = 31

    for (i <- 1 to mapSize) {
      mysqlColumns += (",day_" + i)
    }

    val result = statement.executeQuery("select " + mysqlColumns + " from " + tableName)

    for (i <- 1 to mapSize) {
      resultMap += ((month + "月" + i + "日") -> 0)
    }

    while (result.next()) {

      for (i <- 2 to mapSize) {

        val currentHeat = result.getInt(i)
        val key = month + "月" + (i - 1) + "日"
        val sum = resultMap(key)
        resultMap.put(key, sum + currentHeat)
      }

    }

    resultMap
  }

}
