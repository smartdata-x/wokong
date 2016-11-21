package com.kunyan.wokongsvc.realtimedata.task

import com.kunyan.wokongsvc.realtimedata.DataPattern._
import com.kunyan.wokongsvc.realtimedata._
import logger.HeatLogger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success}

/**
  * Created by sijiansheng on 2016/10/24.
  */
object ReplenshVisit {

  val TABLE_PREFIX_VISIT = "stock_visit"
  val TABLE_PREFIX_SEARCH = "stock_search"
  val TABLE_PREFIX = TABLE_PREFIX_VISIT
  val VISIT_TYPE = (43, 91)
  val SEARCH_TYPE = (0, 42)
  val DATA_TYPE = VISIT_TYPE

  def main(args: Array[String]) {

    /* 初始化xml配置 */
    val xml = XmlHandle(args(2))
    /* 初始化mysql操作句柄 */
    val sqlHandle = MysqlHandle(xml.getElem("mysql_stock", "url"), xml)
    /* 初始化股票的各种别名 */
    val stockalias = getStockAlias(sqlHandle)

    val sc = new SparkConf().setAppName("HeatStatstic")
    val stc = new SparkContext(sc)

    val alias = stc.broadcast(stockalias)

    var fileName: String = null
    val fileNamePath = args(0)
    val dataPath = args(1)
    var prevRDD: RDD[(String, Int)] = null

    while ( {
      fileName = MixTool.obtainFileContent(fileNamePath)
      fileName
    } != null) {

      val staticData = stc.textFile(dataPath + fileName + ".tar.gz")
        .map(x => MixTool.replenish(x, alias.value, DATA_TYPE)).filter(_._2 != 0)
        .reduceByKey(_ + _).cache()

      staticData.coalesce(16).foreachPartition(x => {

        val handle = MysqlHandle(xml.getElem("mysql_stock", "url"), xml)

        x.foreach(y => {

          handle.addCommand(
            s"replace into $TABLE_PREFIX values(\'" + y._1._2 + "\'," + y._1._1.toLong + "," + y._2 + ");"
          ) recover {
            case e: Exception => {
              HeatLogger.exception(e)
            }
          }

          handle.addCommand(
            s"replace into ${TABLE_PREFIX}_old values(\'" + y._1._2 + "\'," + y._1._1.toLong + "," + y._2 + ");"
          ) recover {
            case e: Exception => {
              HeatLogger.exception(e)
            }
          }

          handle.batchExec recover {
            case e: Exception => {
              HeatLogger.exception(e)
            }
          }

        })


      }

      )

      //      staticData.map(z => (z._1._2, z._2)).reduceByKey(_ + _).coalesce(16).foreachPartition(x => {
      //
      //        val handle = MysqlHandle(xml.getElem("mysql_stock", "url"), xml)
      //
      //        x.foreach(y => {
      //
      //          handle.addCommand(
      //            s"update ${TABLE_PREFIX}_month_11 set day_" + fileName.substring(8, 10) + " = " + y._2 + " where stock_code = " + y._1
      //          ) recover {
      //            case e: Exception => {
      //              exceptionLog(e)
      //            }
      //          }
      //
      //          handle.batchExec recover {
      //            case e: Exception => {
      //              exceptionLog(e)
      //            }
      //          }
      //
      //        })
      //
      //      })

    }

    stc.stop()
  }

  def getStockAlias(sqlHandle: MysqlHandle): Tuple2Map = {

    val temp = sqlHandle.execQueryStockAlias(MixTool.SYN_SQL) match {

      case Success(ret) => ret
      case Failure(e) => {
        System.exit(-1)
      }
    }

    temp.asInstanceOf[Tuple2Map]
  }

}
