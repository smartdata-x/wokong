/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/MixTool.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-05-24 11:09
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import scala.collection.mutable

/**
  * Created by wukun on 2016/5/23
  * 大杂烩，一些常量、拼接字符串和解析方法
  */
object MixTool {

  type Tuple2Map = (mutable.HashSet[String], (mutable.Map[String, String], mutable.Map[String, String], mutable.Map[String, String]))

  val VISIT = "stock_visit"
  val ALL_VISIT = "stock_visit_count"

  val SEARCH = "stock_search"
  val ALL_SEARCH = "stock_search_count"

  val FOLLOW = "stock_follow"
  val ALL_FOLLOW = "stock_follow_count"

  //  val STOCK_SQL = "select v_code from stock_info"
  //  val SYN_SQL = "select v_code, v_name_url, v_jian_pin, v_quan_pin from stock_info"
  //  val STOCK_INFO = "select v_code, v_name from stock_info"

  val STOCK_SQL = "select SYMBOL from SH_SZ_CODE"
  val SYN_SQL = "select SYMBOL, SENAMEURL, SESPELL, SEENGNAME from SH_SZ_CODE"
  val STOCK_INFO = "select SYMBOL, SENAME from SH_SZ_CODE"

  def insertTotal(
                   table: String,
                   stamp: Long,
                   count: Int): String = {

    "insert into " + table + " values(" + stamp + "," + count + ");"
  }

  def insertAdd(
                 table: String,
                 code: String,
                 stamp: Long,
                 count: Int): String = {

    "insert into " + table + " values(\'" + code + "\'," + stamp + "," + count + ");"
  }

  def insertCount(
                   table: String,
                   code: String,
                   stamp: Long,
                   count: Int): String = {

    "insert into " + table + " values(\'" + code + "\'," + stamp + "," + count + ");"
  }

  def deleteData(table: String): String = {
    "delete from " + table
  }

  def deleteTime(table: String): String = {
    "delete from " + "update_" + table.slice(6, table.length) + " where update_time <= " + TimeHandle.getPrevTime
  }

  def insertOldCount(
                      table: String,
                      code: String,
                      stamp: Long,
                      count: Int): String = {

    "insert into " + table + " values(\'" + code + "\'," + stamp + "," + count + ");"
  }

  def updateAccum(
                   table: String,
                   code: String,
                   accumulator: Int): String = {

    "update " + table + " set accum = accum + " + accumulator + " where stock_code = " + code
  }

  def updateAccum(
                   table: String,
                   accumulator: Int): String = {

    "update " + table + " set accum = " + accumulator
  }

  def updateMonthAccum(
                        table: String,
                        code: String,
                        month: Int,
                        day: Int,
                        accum: Int): String = {
    "update " + table + month + " set " + "day_" + day + " = " + "day_" + day + "+" + accum + " where stock_code = " + code
  }

  def insertTime(table: String, stamp: Long): String = {
    "insert into " + table + " values(" + stamp + ");"
  }

  def updateMax(table: String, recode: String, max: Int): String = {
    "update " + table + " set " + recode + "=" + max
  }

  def fileName: String = {
    Thread.currentThread.getStackTrace()(2).getFileName
  }

  def rowNum: Int = {
    Thread.currentThread.getStackTrace()(2).getLineNumber
  }

  def stockSearch(stockString: String): String = {

    val elem = stockString.split("\t")
    if (elem.size != 3) {
      "0"
    } else {
      val tp = elem(2).toInt

      val mappedType = {
        if (tp >= 0 && tp <= 42) {
          "1"
        } else {
          "0"
        }
      }

      mappedType
    }
  }

  /**
    * 将不同类型的股票进行归类
    *
    * @param stockString 股票字符串
    * @author wukun
    */
  def stockClassify(
                     stockString: String,
                     alias: Tuple2Map): ((String, String), String) = {

    val elem = stockString.split("\t")

    if (elem.size != 3) {
      (("0", "0"), "0")
    } else {

      val tp = elem(2).toInt
      val mappedType = {

        if (tp >= 0 && tp <= 42) {

          val stockCode = DataPattern.stockCodeMatch(elem(0), alias)
          if (stockCode.compareTo("0") == 0) {
            ((stockCode, "0"), elem(1))
          } else {
            ((stockCode, "2"), elem(1))
          }

        } else if (tp >= 43 && tp <= 91) {

          val stockCode = DataPattern.stockCodeMatch(elem(0), alias)
          if (stockCode.compareTo("0") == 0) {
            ((stockCode, "0"), elem(1))
          } else {
            ((stockCode, "1"), elem(1))
          }

        } else
          (("0", "0"), "0")
      }

      mappedType
    }
  }

  def stockClassified(
                       stockString: String,
                       alias: Tuple2Map): ((String, String), Int) = {
    val elem = stockString.split("\t")

    if (elem.size != 3) {
      (("0", "0"), 0)
    } else {
      val tp = elem(2).toInt
      val mappedType = {
        val stockCode = DataPattern.stockCodeMatch(elem(0).substring(43), alias)

        if (stockCode.compareTo("0") == 0) {
          (("0", "0"), 0)
        } else if (tp >= 0 && tp <= 42) {
          (("0", "0"), 0)
        } else if (tp >= 43 && tp <= 91) {
          ((elem(0).substring(24, 36), stockCode), 1)
        } else {
          (("0", "0"), 0)
        }
      }

      mappedType
    }
  }

  def replenshVisit(
                     stockString: String,
                     alias: Tuple2Map): ((String, String), Int) = {
    val elem = stockString.split("\t")

    if (elem.size != 3) {
      (("0", "0"), 0)
    } else {
      val tp = elem(2).toInt
      val mappedType = {
        val stockCode = DataPattern.stockCodeMatch(elem(0), alias)

        if (stockCode.compareTo("0") == 0) {
          (("0", "0"), 0)
        } else if (tp >= 0 && tp <= 42) {
          (("0", "0"), 0)
        } else if (tp >= 43 && tp <= 91) {
          (((elem(1).toLong / 1000).toString, stockCode), 1)
        } else {
          (("0", "0"), 0)
        }
      }

      mappedType
    }
  }


  def replenish(stockString: String,
                alias: Tuple2Map, dataType: (Int, Int)): ((String, String), Int) = {

    val elem = stockString.split("\t")

    if (elem.size != 3) {
      (("0", "0"), 0)
    } else {
      val tp = elem(2).toInt
      val mappedType = {
        val stockCode = DataPattern.stockCodeMatch(elem(0), alias)

        if (tp >= dataType._1 && tp <= dataType._2) {
          (((elem(1).toLong / 1000).toString, stockCode), 1)
        } else {
          (("0", "0"), 0)
        }
      }

      mappedType
    }
  }

  //  def createTopic(
  //                   zookeeper: String,
  //                   sessionTimeout: Int,
  //                   connectTimeout: Int,
  //                   topic: String,
  //                   repli: Int,
  //                   partitions: Int
  //                 ) {
  //
  //    val zkClient = new ZkClient(
  //      zookeeper,
  //      sessionTimeout,
  //      connectTimeout,
  //      ZKStringSerializer
  //    )
  //    try {
  //      AdminUtils.createTopic(zkClient, topic, partitions, repli)
  //    } catch {
  //      case e: kafka.common.TopicExistsException => {
  //      }
  //    }
  //  }

  def division(num1: Double, num2: Double, size: Int): Int = {
    ((num1 / num2) * size).toInt
  }

  def obtainFileContent(path: String): String = {

    val fileHandle = FileHandle(path)
    val reader = fileHandle.initBuff()
    var (delete, ident, save) = (reader.readLine(), reader.readLine(), "")

    while (ident != null) {
      save += ident + "\n"
      ident = reader.readLine()
    }

    reader.close()

    val writer = fileHandle.initWriter()
    writer.write(save.trim)
    writer.close()
    println(s"删除文件中的：$delete")
    delete
  }

}
