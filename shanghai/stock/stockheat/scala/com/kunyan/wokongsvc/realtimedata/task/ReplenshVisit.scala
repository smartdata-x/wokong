package com.kunyan.wokongsvc.realtimedata.task

import java.io.{FileWriter, File}

import com.kunyan.wokongsvc.realtimedata.DataPattern._
import com.kunyan.wokongsvc.realtimedata._
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.log4j.PropertyConfigurator
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
  val TABLE_PREFIX = TABLE_PREFIX_VISIT
  //  val DATA_TYPE = VISIT_TYPE

  // "E://heat/2016.txt"
  // "hdfs://61.147.114.85:9000/telecom/shdx/search"
  // "E://heat/config.xml"
  // "E://heat/order.csv"

  def main(args: Array[String]) {

    /* 初始化xml配置 */
    val xml = XmlHandle(args(2))
    /* 初始化mysql操作句柄 */
    val sqlHandle = MysqlHandle(xml.getElem("mysql_stock", "url"), xml)
    /* 初始化股票的各种别名 */
    val stockalias = getStockAlias(sqlHandle)
    val sparkConf = new SparkConf().setAppName("HeatStatstic")
    val stc = new SparkContext(sparkConf)

    val alias = stc.broadcast(stockalias)

    var fileName: String = null
    val fileNamePath = args(0)
    val dataPath = args(1)
    val addDataTypes = List[String](TABLE_PREFIX_VISIT, TABLE_PREFIX_SEARCH)

    //    addDataTypes.foreach { dateType =>
    //
    //      var DATE_TYPE = SEARCH_TYPE
    //      var TABLE_PREFIX = TABLE_PREFIX_SEARCH
    //
    //      if (dateType == TABLE_PREFIX_VISIT) {
    //        DATE_TYPE = VISIT_TYPE
    //        TABLE_PREFIX = TABLE_PREFIX_VISIT
    //      }

    while ( {
      fileName = MixTool.obtainFileContent(fileNamePath)
      fileName
    } != null) {

      addDataTypes.foreach { dateType =>

        var DATE_TYPE = SEARCH_TYPE
        var TABLE_PREFIX = TABLE_PREFIX_SEARCH

        if (dateType == TABLE_PREFIX_VISIT) {
          DATE_TYPE = VISIT_TYPE
          TABLE_PREFIX = TABLE_PREFIX_VISIT
        }

        try {
          val staticData = stc.textFile(dataPath + fileName + ".tar.gz")
            .map(x => MixTool.replenish(x, alias.value, DATE_TYPE)).filter(_._1._2 != "0")
            .reduceByKey(_ + _).collect()

          saveAsCSVWithSTC(staticData, s"/home/sijiansheng/heatcount/$TABLE_PREFIX/$fileName.csv")
        } catch {
          case e: InvalidInputException =>
            logger.warn(e)
            logger.warn(e.getMessage)
        }

      }

    }

    stc.stop()
  }

  def saveStockAndCount(rdd: RDD[((String, String), Int)], path: String): Unit = {
    rdd.map(x => (x._1._2, x._2)).reduceByKey(_ + _).saveAsTextFile(path)
  }

  def saveAsCSVWithSTC(rdd: Array[((String, String), Int)], path: String) {

    val file = new File(path)
    val writer = new FileWriter(path, false)
    rdd.foreach { cell =>
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

      handle.addCommand(
        s"replace into $TABLE_PREFIX values(\'" + y._1._2 + "\'," + y._1._1.toLong + "," + y._2 + ");"
      ) recover {
        case e: Exception =>
          exception(e)
      }

      handle.addCommand(
        s"replace into ${TABLE_PREFIX}_old values(\'" + y._1._2 + "\'," + y._1._1.toLong + "," + y._2 + ");"
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
