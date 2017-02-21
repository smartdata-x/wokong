package com.kunyan.wokongsvc.realtimedata

import com.kunyan.wokongsvc.realtimedata.DataPattern.Tuple2Map
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success}

/**
  * Created by wukun on 2016/08/17
  * 热度统计类
  */
object HeatSupply {

  def main(args: Array[String]) {

    /* 初始化xml配置 */
    val xml = XmlHandle(args(2))
    /* 初始化mysql操作句柄 */
    val sqlHandle = MysqlHandle(xml.getElem("mysql_stock", "url"), xml)
    /* 初始化股票的各种别名 */
    val stockalias = sqlHandle.execQueryStockAlias(MixTool.SYN_SQL) match {

      case Success(ret) => ret
      case Failure(e) =>
        System.exit(-1)
    }

    val sc = new SparkConf().setAppName("HeatStatstic")
    val stc = new SparkContext(sc)

    val alias = stc.broadcast(stockalias.asInstanceOf[Tuple2Map])

    var file: String = null
    val fileNamePath = args(0)
    val dataPath = args(1)
    var prevRDD: RDD[(String, Int)] = null
    println(fileNamePath)
    println(dataPath)

    while ( {

      file = MixTool.obtainFileContent(fileNamePath)
      file

    } != null) {

      stc.textFile(dataPath + file + ".tar.gz")
        .map(x => {
          val value = MixTool.stockClassified(x, alias.value)
          value
        })
        .filter(x => {
          if (x._2 == 0) {
            false
          } else {
            true
          }
        })
        .reduceByKey(_ + _)
        .coalesce(16)
        .foreachPartition(x => {

          val handle = MysqlHandle(xml.getElem("mysql_stock", "url"), xml)
          x.foreach(y => {

            val tamp = TimeHandle.timeTamp(y._1._1, "yyyyMMddHHmm")


            handle.addCommand(
              MixTool.insertOldCount("stock_visit" + "_old", y._1._2, tamp, y._2)
            ) recover {
              case e: Exception =>
            }

            handle.addCommand(
              MixTool.insertOldCount("stock_visit" + "_month_10", y._1._2, tamp, y._2)
            ) recover {
              case e: Exception =>
                exception(e)
            }

            handle.batchExec recover {
              case e: Exception =>
                exception(e)
            }
          })
        })
    }
  }
}
