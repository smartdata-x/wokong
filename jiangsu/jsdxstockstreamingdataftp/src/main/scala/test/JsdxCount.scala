package test

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import util.FileUtil
import java.util
import java.util.{Date, Properties}
import java.util.regex.Pattern

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/8/30.
  * 江苏电信实时stock统计类
  */
object JsdxCount {

  var stockCodes:java.util.List[String] = new util.ArrayList[String]()
  var nameUrls = mutable.MutableList[String]()
  var jianPins = mutable.MutableList[String]()
  var quanPins = mutable.MutableList[String]()

  def getTime(timeStamp: String): String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val date: String = sdf.format(new Date(timeStamp.toLong))
    date
  }
  def getConfInfo(line :String):(String,String) = {
    val  lineSplits: Array[String] = line.split("=")
    val  attr = lineSplits(0)
    val confValue = lineSplits(1)
    (attr,confValue)
  }

  def flatMapFun(line: String): mutable.MutableList[String] = {

    val lineList: mutable.MutableList[String] = mutable.MutableList[String]()
    try {
      val lineSplits: Array[String] = line.split ("\t")
      if (lineSplits.length < 3) {
        return lineList
      }
      val stockCode = lineSplits(0)
      val timeStamp = lineSplits(1)
      val visitWebsite = lineSplits(2)
      val hour = getTime (timeStamp)

      /** visit */
      if ((!" ".equals (stockCode)) && timeStamp != null && visitWebsite != null) {
        if (visitWebsite.charAt (0) >= '0' && visitWebsite.charAt (0) <= '5') {
          lineList += ("hash:visit:" + hour + "kunyan" + stockCode + "kunyan" + visitWebsite)
        } else if (visitWebsite.charAt (0) >= '6' && visitWebsite.charAt (0) <= '9') {
          lineList += ("hash:search:" + hour + "kunyan" + stockCode + "kunyan" + visitWebsite)
        }
      }
    } catch {
      case e:Exception => println(s"flatMapFun error:$line")
    }
    lineList
  }

  def flatMapFunLater(line: String): mutable.MutableList[String] = {

    val lineList: mutable.MutableList[String] = mutable.MutableList[String]()
    try {
      val lineSplits: Array[String] = line.split ("\t")
      if (lineSplits.length < 3) {
        return lineList
      }
      val stockCode = lineSplits(0)
      val timeStamp = lineSplits(1)
      val visitWebsite = lineSplits(2)
      val hour = getTime (timeStamp)

      if ((!" ".equals (stockCode)) && timeStamp != null && visitWebsite != null) {
        if(visitWebsite forall Character.isDigit){
          if (visitWebsite.toInt >= 42  && visitWebsite.toInt <= 95) {
            lineList += ("hash:visit:" + hour + "kunyan" + stockCode + "kunyan" + visitWebsite)
          } else if (visitWebsite.toInt >= 0 && visitWebsite.toInt <= 41) {
            lineList += ("hash:search:" + hour + "kunyan" + stockCode + "kunyan" + visitWebsite)
          }
        }

      }
    } catch {
      case e:Exception => println(s"flatMapFun error:$line")
    }
    lineList
  }

  def mapFunct(s:String):mutable.MutableList[String] = {

    var words: mutable.MutableList[String] = new mutable.MutableList[String]()
    try {
      if (s.startsWith ("hash:search:")) {
        val keys: Array[String] = s.split ("kunyan")
        val visitWebsite = keys(2)

        if (keys.length < 2) {
          return words
        } else {
          var keyWord = keys (1)
          val hours = keys (0).split (":")(2)
          if (keyWord.length < 4) {
            return words
          }
          val firstChar = keyWord.charAt (0)
          if (firstChar >= '0' && firstChar <= '9') {
            if (keyWord.length < 6) {
              return words
            } else {
              val iterator: util.Iterator[String] = stockCodes.iterator ()
              while (iterator.hasNext) {
                val stockCode = iterator.next ()
                if (stockCode.contains (keyWord)) {
                  words += ("hash:search:" + hours + "kunyan" + stockCode)
                  words += ("visitWebsite:" + visitWebsite )
                }
              }
            }
          } else if (firstChar == '%') {
            val pattern: Pattern = Pattern.compile ("%.*%.*%.*%.*%.*%.*%.*%")
            if (!pattern.matcher (keyWord).find ()) {
              return words
            } else {
              keyWord = keyWord.toUpperCase ()
              var index: Int = 0
              for (nameUrl <- nameUrls) {
                index += 1
                if (nameUrl.contains (keyWord)) {
                  words += (keys (0) + "kunyan" + stockCodes.get (index - 1))
                  words += ("visitWebsite:"+ visitWebsite )
                }
              }
            }

          } else if ((firstChar >= 'A' && firstChar <= 'Z') || (firstChar >= 'a' && firstChar <= 'z')) {
            keyWord = keyWord.toLowerCase ()
            var index: Int = 0
            for (jianPin <- jianPins) {
              index += 1
              if (jianPin.contains (keyWord)) {
                words += (keys (0) + "kunyan" + stockCodes.get (index - 1))
                words += ("visitWebsite:" + visitWebsite )
              }
            }
            if (index == 0) {
              for (quanPin <- quanPins) {
                index += 1
                if (quanPin.contains (keyWord)) {
                  words += (keys (0) + "kunyan" + stockCodes.get (index - 1))
                  words += ("visitWebsite:" + visitWebsite )
                }
              }
            }
          }
        }
      } else if (s.startsWith ("hash:visit:")) {
        val keys: Array[String] = s.split ("kunyan")
        val visitWebsite = keys(2)
        if (keys.length < 2) {
          return words
        } else {
          val keyWord = keys (1)
          if (stockCodes.contains (keyWord)) {
            words += s
            words += ("visitWebsite:" + visitWebsite )
          }
        }
      }
    } catch {
      case e:Exception => println(s"mapFunct error:$s")
    }
    words
  }

  val sparkConf = new SparkConf()
    .setAppName("JsdxCount")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "2000")
    .set("spark.driver.allowMultipleContexts", "true")
  //  .setMaster("local")

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  val url = "jdbc:mysql://61.147.114.67:3306/stock?user=root&password=dataservice2015&useUnicode=true&characterEncoding=utf8"

  val  properties = new Properties()
  properties.setProperty("driver","com.mysql.jdbc.Driver")

  def getTableData(tableName:String): Unit = {
    sqlContext.read.jdbc(url,tableName,properties).foreach(row =>{
      stockCodes.add(row(0).toString)
      nameUrls.+=(row(2).toString)
      jianPins.+=(row(3).toString)
      quanPins.+=(row(4).toString)
    })
  }

  def main(args: Array[String]) {

    val Array(dataDir, date, saveDir) = args

    // 写入本地文件中(备份用)
    val nowDate: String = date

    getTableData("stock_info")


    val dataValue = sc.textFile(dataDir + "/" + date)

    val length = stockCodes.size()
    println(s"stockCode size:$length")

    var lines:RDD[String] = null

    lines = dataValue.flatMap(flatMapFunLater)

    val COUNT = lines.count()
    println(s"lines size:$COUNT")

    System.out.println("flatmap:start")
    val data = lines.flatMap(mapFunct).map((_,1)).reduceByKey(_+_)

    // 搜索和查看的统计
    val result2 = data.filter(x => x._1.startsWith("hash:")).map { line =>
      val arr = line._1.split("kunyan")
      val stock = arr(1)
      val hour = arr(0).split(":")(2)
      val typeString = arr(0).split(":")(1)
      (typeString,(stock,hour,line._2))
    }

    val fileDataCount = result2.filter(_._2._2.contains(nowDate)).map(x => (x._1, 1)).reduceByKey(_ + _).map(x => x._1 + "\t" + x._2).collect()

    val savePath = saveDir  +"/" + date

    FileUtil.mkDir(savePath)

    FileUtil.write(savePath + "/" + "count_"+ date, fileDataCount)


  }

}
