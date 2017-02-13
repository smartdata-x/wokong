package com.kunyan.wokongsvc.realtimedata.task

import java.io.{FileWriter, File}

import com.kunyan.wokongsvc.realtimedata
import com.kunyan.wokongsvc.realtimedata.DataPattern._
import com.kunyan.wokongsvc.realtimedata._
import com.kunyan.wokongsvc.realtimedata.task.comparison.DayStatics
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success}

/**
  * Created by sijiansheng on 2016/10/24.
  */
object ReplenshVisit {

  val TABLE_PREFIX_VISIT = "stock_visit"
  val TABLE_PREFIX_SEARCH = "stock_search"
  val VISIT_TYPE = (43, 91)
  val SEARCH_TYPE = (0, 42)
  //  val VISIT_TYPE = (0, 5)
  //  val SEARCH_TYPE = (6, 9)
  val TABLE_PREFIX = TABLE_PREFIX_VISIT

  //  val DATA_TYPE = VISIT_TYPE
  // "E://heat/2016.txt"
  // "hdfs://61.147.114.85:9000/telecom/shdx/search"
  // "E://heat/config.xml"
  // "E://heat/order.csv"

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("HeatStatstic").setMaster("local")
    val stc = new SparkContext(sparkConf)

    /* 初始化xml配置 */
    val xml = XmlHandle(args(0))
    /* 初始化mysql操作句柄 */
    val sqlHandle = MysqlHandle(xml.getElem("mysql_stock", "url"), xml)
    /* 初始化股票的各种别名 */
    val stockalias = getStockAlias(sqlHandle)

    val alias = stc.broadcast(stockalias)
    val allDataTypes = List[String](TABLE_PREFIX_VISIT)
    val flag = args(1)
    val message = args(2)

    logger.warn("开始执行")

    flag match {

      case "file" => executeByFile(message, stc, alias, allDataTypes, xml: XmlHandle)
      case "time" => executeByTime(message, stc, alias, allDataTypes, xml: XmlHandle)
      case _ =>
        logger.error("文件标志错误，文件格式为file或者time")
        System.exit(-1)
    }

    stc.stop()

  }

  def executeByTime(message: String, stc: SparkContext, alias: Broadcast[Tuple2Map], allDataTypes: List[String], xml: XmlHandle): Unit = {

    val messageArray = message.split(",")
    val times = messageArray(0).split(":")
    val timeIntervals = DayStatics.getTimeByStartAndEnd(times(0), times(1))
    val orderFilePrefix = messageArray(1)
    timeIntervals.foreach(fileName => execute(stc, orderFilePrefix, fileName, allDataTypes: List[String], alias, xml: XmlHandle))
  }

  def executeByFile(message: String, stc: SparkContext, alias: Broadcast[Tuple2Map], allDataTypes: List[String], xml: XmlHandle): Unit = {

    val cells = message.split(",")
    val nameFilePath = cells(0)
    var fileName: String = null

    while ( {
      fileName = MixTool.obtainFileContent(nameFilePath)
      fileName
    } != null) {

      val cells = message.split(",")
      val orderFilePrefix = cells(1)

      execute(stc, orderFilePrefix, fileName, allDataTypes: List[String], alias, xml)
    }
  }

  def execute(stc: SparkContext, dataPath: String, fileName: String, allDataTypes: List[String], alias: Broadcast[Tuple2Map], xml: XmlHandle): Unit = {

    allDataTypes.foreach {

      dateType =>

        var DATE_TYPE = SEARCH_TYPE
        var TABLE_PREFIX = TABLE_PREFIX_SEARCH

        if (dateType == TABLE_PREFIX_VISIT) {
          DATE_TYPE = VISIT_TYPE
          TABLE_PREFIX = TABLE_PREFIX_VISIT
        }

        try {

          //          val staticData = stc.textFile(dataPath + fileName + ".tar.gz")
          //          val staticData = stc.textFile(dataPath + fileName)
          //            .map(x => MixTool.replenish(x, alias.value, DATE_TYPE)).filter(_._1._2 != "0")
          //            .reduceByKey(_ + _).collect()
          val staticData = stc.textFile(dataPath + fileName)
            .map(x => MixTool.replenish(x, alias.value, DATE_TYPE)).filter(_._1._2 != "0")
            .reduceByKey(_ + _)

          //          saveAsCSVWithSTC(staticData, s"/home/telecom/shdx/data/search_offline_heat/heat_data/$fileName.csv")
          //          saveAsCSVWithSTC(staticData, s"E://heat/$TABLE_PREFIX/$fileName.csv")
          staticData.foreachPartition(x => saveToMysql(xml, x))
        } catch {
          case e: InvalidInputException =>
            logger.warn(e)
            logger.warn(e.getMessage)
        }

    }

  }

  def saveStockAndCount(rdd: RDD[((String, String), Int)], path: String): Unit = {
    rdd.map(x => (x._1._2, x._2)).reduceByKey(_ + _).saveAsTextFile(path)
  }

  def saveAsCSVWithSTC(rdd: Array[((String, String), Int)], path: String) {

    logger.warn(s"执行写文件方法，文件名是$path########################################################################")
    val file = new File(path)
    val writer = new FileWriter(path, false)

    rdd.foreach {
      cell =>
        writer.write(cell._1._2 + "," + cell._1._1 + "," + cell._2 + "\n")
    }

    writer.close()
  }


  def checkFile(file: File): Unit = {
    if (file.exists()) {
      file.delete()
    }
  }

  def saveToMysql(xml: XmlHandle, x: (Iterator[((String, String), Int)])): Unit = {

    val handle = MysqlHandle(xml.getElem("mysql_stock", "url"), xml)

    x.foreach(y => {
      val time = y._1._1.toLong
      val count = y._2
      val stock = y._1._2
      //实时表
      handle.addCommand(
        s"replace into $TABLE_PREFIX(stock_code,timeTamp,count) values(\'" + stock + "\'," + time + "," + count + ");"
      ) recover {
        case e: Exception =>
          exception(e)
      }
      //old表
      handle.addCommand(
        s"replace into ${TABLE_PREFIX}_old(stock_code,timeTamp,count) values(\'" + stock + "\'," + time + "," + count + ");"
      ) recover {
        case e: Exception =>
          exception(e)
      }
      //month表 todo 这个没有完善，现在只是写死的内容
      handle.addCommand(
        s"insert into ${TABLE_PREFIX}_month_1(stock_code,day_17) values(\'$stock\',$count) on duplicate key update day_17 = (day_17+$count);"
      ) recover {
        case e: Exception =>
          exception(e)
      }

      handle.batchExec recover {
        case e: Exception =>
          exception(e)
      }

    })

  }

  def getStockAlias(sqlHandle: MysqlHandle): Tuple2Map = {

    val temp = sqlHandle.execQueryStockAlias(MixTool.SYN_SQL) match {

      case Success(ret) => ret
      case Failure(e) =>
        System.exit(-1)
    }

    temp.asInstanceOf[Tuple2Map]
  }

}
