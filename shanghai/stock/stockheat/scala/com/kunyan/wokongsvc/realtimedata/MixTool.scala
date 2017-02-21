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

  val STOCK_SQL = "select SYMBOL from SH_SZ_CODE"
  val SYN_SQL = "select SYMBOL, SENAMEURL, SESPELL, SEENGNAME from SH_SZ_CODE"
  val STOCK_INFO = "select SYMBOL, SENAME from SH_SZ_CODE"

  def insertTotal(
                   table: String,
                   stamp: Long,
                   count: Int): String = {

    s"insert into $table values($stamp,$count) on duplicate key update count = (count+ $count);"
  }

  def insertAdd(
                 table: String,
                 code: String,
                 stamp: Long,
                 count: Int): String = {

    s"insert into $table(stock_code,timeTamp,count) values(\'$code\',$stamp,$count) on duplicate key update count = (count + $count);"
  }

  def insertCount(
                   table: String,
                   code: String,
                   stamp: Long,
                   count: Int): String = {

    s"insert into $table(stock_code,timeTamp,count) values(\'$code\',$stamp,$count) on duplicate key update count = (count + $count);"
  }

  def insertOrUpdateAccum(
                   table: String,
                   code: String,
                   stamp: Long,
                   accum: Int): String = {

    s"insert into $table(stock_code,timeTamp,accum) values(\'$code\',$stamp,$accum) on duplicate key update accum = (accum + $accum);"
  }

  def deleteData(table: String): String = {
    s"delete from $table"
  }

  def deleteTime(table: String): String = {
    s"delete from update_${table.slice(6, table.length)} where update_time <= ${TimeHandle.getPrevTime};"
  }

  def insertOldCount(
                      table: String,
                      code: String,
                      stamp: Long,
                      count: Int): String = {

    s"insert into $table(stock_code,timeTamp,count) values(\'$code\',$stamp,$count) on duplicate key update count = (count+$count);"
  }

  def updateAccum(
                   table: String,
                   code: String,
                   accumulator: Int): String = {

    s"update $table set accum = accum + $accumulator where stock_code = $code;"
  }

  def updateAccum(
                   table: String,
                   accumulator: Int): String = {

    s"update $table set accum = $accumulator;"
  }

  def updateMonthAccum(
                        tablePrefix: String,
                        code: String,
                        month: Int,
                        day: Int,
                        accum: Int): String = {
    s"insert into $tablePrefix$month (stock_code,day_$day) values(\'$code\',$accum) on duplicate key update day_$day = (day_$day+$accum);"
  }


  def insertTime(table: String, stamp: Long): String = {
    s"insert into $table values($stamp);"
  }

  def updateMax(table: String, recode: String, max: Int): String = {
    s"update $table set $recode=$max;"
  }

  def fileName: String = {
    Thread.currentThread.getStackTrace()(2).getFileName
  }

  def rowNum: Int = {
    Thread.currentThread.getStackTrace()(2).getLineNumber
  }

  /**
    * 将不同类型的股票进行归类
    *
    * @param stockString 股票字符串
    * @author wukun
    */
  def stockClassify(
                     stockString: String,
                     alias: Tuple2Map,
                     needFilter: Boolean,
                     levelStandard: Int,
                     currentTamp: Long): ((String, String), Long) = {

    val elems = stockString.split("\t")

    if (elems.size < 3) {
      (("0", "0"), 0L)
    } else if (!needFilter) {
      classifyFunctionByThreeElem(elems, alias, currentTamp)
    } else if (needFilter) {
      classifyFunctionByFourElem(elems, alias, levelStandard, currentTamp)
    } else {
      (("0", "0"), 0L)
    }

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

  def classifyFunctionByThreeElem(elem: Array[String], alias: Tuple2Map, currTamp: Long): ((String, String), Long) = {

    val tp = elem(2).toInt
    val time = TimeHandle.getMinuteTimeStamp(elem(1).toLong)

    val mappedType = {

      if (tp >= 0 && tp <= 42 && time <= currTamp) {

        val stockCode = DataPattern.stockCodeMatch(elem(0), alias)
        if (stockCode.compareTo("0") == 0) {
          ((stockCode, "0"), time)
        } else {
          ((stockCode, "2"), time)
        }

      } else if (tp >= 43 && tp <= 91) {

        val stockCode = DataPattern.stockCodeMatch(elem(0), alias)
        if (stockCode.compareTo("0") == 0) {
          ((stockCode, "0"), time)
        } else {
          ((stockCode, "1"), time)
        }

      } else
        (("0", "0"), 0L)

    }

    mappedType
  }

  def classifyFunctionByFourElem(elems: Array[String], alias: Tuple2Map, levelStandard: Int, currTamp: Long): ((String, String), Long) = {

    if (elems(3).toInt <= levelStandard) {
      classifyFunctionByThreeElem(elems, alias, currTamp)
    } else {
      (("0", "0"), 0L)
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

    //    logger.warn(stockString)
    val elem = stockString.split(" ")
    //    val elem = stockString.split("\t")
    val stockSumType = elem(3)

    if (elem.size <= 3) {
      (("0", "0"), 0)
    } else if (stockSumType.toInt > 2) {
      (("0", "0"), 0)
    } else {
      val tp = elem(2).toInt

      val mappedType = {

        val stockCode = DataPattern.stockCodeMatch(elem(0), alias)

        if (tp >= dataType._1 && tp <= dataType._2) {
          (((elem(1).toLong / 1000 / 60 * 60).toString, stockCode), 1)
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
