package com.kunyan.wokongsvc.realtimedata.task.comparison

import java.sql.{DriverManager, Statement}

import com.kunyan.wokongsvc.realtimedata
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.xml.XML

/**
  * Created by sijiansheng on 2017/1/9.
  */
object ChangeMysqlData extends Serializable {

  def main(args: Array[String]) {

    lazy val NORMAL = "normal"
    lazy val VISIT = "visit"
    lazy val OTHER = "other"
    val sparkConf = new SparkConf().setAppName("ChangeStockMysql").setMaster("local")
    val stc = new SparkContext(sparkConf)
    val xml = XML.loadFile(args(1))
    lazy val conn = DriverManager.getConnection((xml \ "mysql_stock" \ "url").text, (xml \ "mysql_stock" \ "user").text, (xml \ "mysql_stock" \ "password").text)
    lazy val statement = conn.createStatement()
    //    val stockType = "visit"
    val result = stc.textFile(args(0)).map(line => {

      val cells = line.split(" ")
      val stockType = cells(5)

      if (stockType != VISIT) {
        null
      } else {
        val stock = cells(3)
        val dayTime = cells(4).substring(0, 10)
        val count = cells(7).toLong
        val normalFlag = cells(6)
        //      return (s"$dayTime $stock $normalFlag", count) todo 有一个疑问，加return后为什么reduceByKey回报异常，加return和不加return有什么区别
        (s"$dayTime $stock $normalFlag", count)
      }

    }).filter(_ != null).reduceByKey(_ + _).persist(StorageLevel.MEMORY_AND_DISK)

    val unnorResult = result.map(cell => filterFlag(NORMAL, cell)).filter(_ != null).collect.toMap

    val allResult = result.map(cell => filterFlag(OTHER, cell)).filter(_ != null).reduceByKey(_ + _)
      .foreach(cell => {

        val timeAndStock = cell._1

        if (unnorResult.contains(timeAndStock)) {

          val allCount = cell._2
          val timesArray = timeAndStock._1.split("-")
          val year = timesArray(0)
          val month = timesArray(1).toInt.toString
          val day = timesArray(2).toInt.toString
          val stock = timeAndStock._2
          val unCount = unnorResult(timeAndStock).toDouble
          val percentage = 1.0 - unCount / allCount

          realtimedata.logger.warn(s"################################################stock:$stock,time:${timeAndStock._1},unCount:$unCount,allCount:$allCount,percentage:$percentage")
          executeUpdateMysql(statement, year, month, day, stock, percentage)
        }

      })

    statement.close()
    conn.close()
    stc.stop()
  }

  def executeUpdateMysql(statement: Statement, year: String, month: String, day: String, stock: String, percentage: Double): Unit = {
    statement.execute(s"update stock_visit_month_$month set day_$day = CAST(day_$day * $percentage as signed) where stock_code = $stock")
  }

  def filterFlag(flag: String, cell: (String, Long)): ((String, String), Long) = {

    val cellArray = cell._1.split(" ")
    val cellFlag = cellArray(2)

    if (cellFlag != flag) {
      ((cellArray(0), cellArray(1)), cell._2)
    } else {
      null
    }

  }
}
