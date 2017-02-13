package com.kunyan.wokongsvc.realtimedata.task.topnews

import java.io.File

import com.kunyan.wokongsvc.realtimedata
import jxl.Workbook
import jxl.format.{UnderlineStyle, Colour, Alignment}
import jxl.write.{WritableCellFormat, WritableFont, Label, WritableSheet}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, Tuple}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.xml.XML

/**
  * Created by sijiansheng on 2017/1/16.
  */
object TopNews {

  val CHANGE_PERCENT = "change_percent_"
  val VOLUME = "volume_"
  type LineType = Tuple7[String, String, String, Int, String, String, String]
  type MAPList = mutable.LinkedHashMap[String, mutable.LinkedHashMap[String, ListBuffer[String]]]

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("TopNews").setMaster("local")
    val stc = new SparkContext(sparkConf)
    val xml = XML.loadFile(args(1))
    val orderPathPrefix = args(2)
    val redisMessage = xml \ "redis"
    RedisHandler.init((redisMessage \ "ip").text, (redisMessage \ "port").text.toInt, (redisMessage \ "auth").text, (redisMessage \ "db").text.toInt)
    lazy val redis = RedisHandler.getInstance().getJedis
    val result: MAPList = mutable.LinkedHashMap[String, mutable.LinkedHashMap[String, ListBuffer[String]]]()
    val resultMap = stc.textFile(args(0)).collect().foreach(line => execute(redis: Jedis, line, result))
    realtimedata.logger.warn(s"############################################################结果的map集合的数量为：${result.size}")
    writeToExcel(result, orderPathPrefix)
  }

  def writeToExcel(resultMap: MAPList, pathPrefix: String): Unit = {

    realtimedata.logger.warn("开始执行写excel程序，###################################################################")

    resultMap.foreach(map1 => {

      val month = map1._1
      val book = Workbook.createWorkbook(new File(pathPrefix + month + ".xls"))

      var sheetIndex = 0

      map1._2.foreach(map2 => {
        val date = map2._1
        val sheet = book.createSheet(date, sheetIndex)
        sheetIndex += 1

        addFirstLine(sheet)

        var lineStartIndex = 1
        val columnStartIndex = 0

        map2._2.foreach(lineString => {
          val lineList = lineString.split("\t").toList
          addLine(lineStartIndex, sheet, lineList, columnStartIndex)
          lineStartIndex += 1
        })

      })


      book.write()
      book.close()
    })
  }

  def addFirstLine(sheet: WritableSheet): Unit =
    addLine(0, sheet, List("新闻标题", "正负面", "新闻热度", "股票代码", "涨跌幅", "交易量"), 0)

  def addLine(lineRank: Int, sheet: WritableSheet, lineList: List[String], columnStartIndex: Int): Unit = {

    var x = columnStartIndex

    lineList.foreach(column => {
      //      val wcf = new WritableCellFormat()
      //      val wf = new WritableFont(WritableFont.ARIAL, 14, WritableFont.BOLD, false, UnderlineStyle.NO_UNDERLINE, Colour.BLACK);
      val wcf = new WritableCellFormat()
      wcf.setAlignment(Alignment.CENTRE)
      val label = new Label(x, lineRank, column, wcf)
      sheet.addCell(label)
      x += 1
    })

  }


  def execute(redis: Jedis, line: String, result: MAPList): Unit = {

    realtimedata.logger.info(line)
    val lineResult = processLine(line, "\t", redis)
    val date = lineResult._1
    val month = date.substring(0, 7)
    val otherMessage = s"${lineResult._2}\t${lineResult._3}\t${lineResult._4}\t${lineResult._5}\t${lineResult._6}\t${lineResult._7}"

    if (result.contains(month)) {

      val secondMap = result(month)

      if (secondMap.contains(date)) {
        secondMap(date) += otherMessage
      } else {
        secondMap.put(date, ListBuffer(otherMessage))
      }

    } else {
      result.put(month, mutable.LinkedHashMap(date -> ListBuffer(otherMessage)))
    }

  }

  def processLine(line: String, splitFlag: String, redis: Jedis): LineType = {

    val lineArray = line.split(splitFlag)
    val date = lineArray(0)
    //    val url = lineArray(1)
    val title = lineArray(2)
    val emotion = lineArray(3)
    var newEmotion = "待定"

    newEmotion = emotion match {
      case "0" => "负面"
      case "1" => "正面"
      case _ =>
        realtimedata.logger.warn(s"情感匹配错误，情感结果是:$emotion")
        "无结果"
    }

    val heat = lineArray(4).toInt
    val stock = lineArray(5)
    realtimedata.logger.warn(s"date:$date,stock:$stock")
    val changePercent = getChangePercent(redis, date, stock)
    val volumn = getVolume(redis, date, stock)
    var newVolumn = volumn.toString
    var newChangePercent = changePercent.toString

    if (changePercent == Double.MaxValue) {
      newChangePercent = "无数据"
    }

    if (volumn == Double.MaxValue) {
      newVolumn = "无数据"
    }

    Tuple7(date, title, newEmotion, heat, stock, newChangePercent, newVolumn)
  }

  def getAllChangePercent(redis: Jedis, date: String): collection.immutable.Map[String, Double] = {
    getAllData(redis, s"$CHANGE_PERCENT$date")
  }

  def getAllVolume(redis: Jedis, date: String): collection.immutable.Map[String, Double] = {
    getAllData(redis, s"$VOLUME$date")
  }

  def getVolume(redis: Jedis, date: String, stock: String): Double = {
    getDateByKey(redis, s"$VOLUME$date", stock)
  }

  def getChangePercent(redis: Jedis, date: String, stock: String): Double = {
    getDateByKey(redis, s"$CHANGE_PERCENT$date", stock)
  }

  def getDateByKey(redis: Jedis, redisKey: String, value: String): Double = {

    val redisResult = redis.zscore(redisKey, value)

    if (redisResult != null) {
      redisResult
    } else {
      Double.MaxValue
    }
  }

  def getAllData(redis: Jedis, redisKey: String): collection.immutable.Map[String, Double] = valueAndScoreToMap(redis.zrangeByScoreWithScores(redisKey, Double.MinValue, Double.MaxValue)).toMap

  def valueAndScoreToMap(set: java.util.Set[Tuple]): mutable.Map[String, Double] = {

    val resultMap = mutable.Map[String, Double]()
    val iterator = set.iterator

    while (iterator.hasNext) {
      val valueAndScore = iterator.next()
      resultMap += (valueAndScore.getElement -> valueAndScore.getScore)
    }

    resultMap
  }

}
