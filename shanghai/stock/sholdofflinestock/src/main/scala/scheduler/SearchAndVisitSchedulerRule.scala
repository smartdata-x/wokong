package scheduler

import java.text.SimpleDateFormat
import java.util
import util.{Date, Properties}
import java.util.regex.Pattern

import _root_.util.FileUtil
import  _root_.log.PrismLogger

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Created by C.J.YOU on 2016/5/27.
  * 访问和搜索 主程序入口
  */
object SearchAndVisitSchedulerRule {

  var stockCodes:java.util.List[String] = new util.ArrayList[String]()
  var nameUrls = mutable.MutableList[String]()
  var jianPins = mutable.MutableList[String]()
  var quanPins = mutable.MutableList[String]()


  /**
    * 解析配置文件信息的信息
    * @param line 配置信息
    * @return  配置属性和对应的值
    */
  def getConfInfo(line :String):(String,String) = {

    val  lineSplits: Array[String] = line.split("=")
    val  attr = lineSplits(0)
    val confValue = lineSplits(1)

    (attr,confValue)

  }

  def getTime(timeStamp: String): String = {

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val date: String = sdf.format(new Date(timeStamp.toLong))

    date

  }

  /**
    * 判断原始数据是搜索还是查看：最早的匹配规则处理函数
    * @param line 原始数据
    * @return 区分后的数据集合
    */

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

      /** 查看、搜索的判断 */
      if ((!" ".equals (stockCode)) && timeStamp != null && visitWebsite != null) {

        if (visitWebsite.charAt (0) >= '0' && visitWebsite.charAt (0) <= '5') {
          lineList += ("hash:visit:" + hour + "_kunyan_" + stockCode + "_kunyan_" + visitWebsite)
        } else if (visitWebsite.charAt (0) >= '6' && visitWebsite.charAt (0) <= '9') {
          lineList += ("hash:search:" + hour + "_kunyan_" + stockCode + "_kunyan_" + visitWebsite)
        }

      }

    } catch {
      case e:Exception =>
        PrismLogger.error(s"flatMapFun error:$line")
    }

    lineList

  }

  /**
    * 判断原始数据是搜索还是查看: 匹配规则增加后的处理函数
    * @param line 原始数据
    * @return 区分后的数据集合
    */
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
      val hour = getTime(timeStamp)

      if ((!" ".equals (stockCode)) && timeStamp != null && visitWebsite != null) {

        if(visitWebsite forall Character.isDigit) {

          if (visitWebsite.toInt >= 42  && visitWebsite.toInt <= 95) {
            lineList += ("hash:visit:" + hour + "_kunyan_" + stockCode + "_kunyan_" + visitWebsite)
          } else if (visitWebsite.toInt >= 0 && visitWebsite.toInt <= 41) {
            lineList += ("hash:search:" + hour + "_kunyan_" + stockCode + "_kunyan_" + visitWebsite)
          }

        }

      }
    } catch {
      case e:Exception =>
        PrismLogger.error(s"flatMapFun error:$line")
    }

    lineList

  }

  def mapFunct(s:String):mutable.MutableList[String] = {

    var words: mutable.MutableList[String] = new mutable.MutableList[String]()

    try {

      if (s.startsWith ("hash:search:")) {

        val keys: Array[String] = s.split ("_kunyan_")
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

                  words += ("hash:search:" + hours + "_kunyan_" + stockCode)
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

                  words += (keys (0) + "_kunyan_" + stockCodes.get (index - 1))
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

                words += (keys (0) + "_kunyan_" + stockCodes.get (index - 1))
                words += ("visitWebsite:" + visitWebsite )

              }
            }

            if (index == 0) {

              for (quanPin <- quanPins) {

                index += 1

                if (quanPin.contains (keyWord)) {

                  words += (keys (0) + "_kunyan_" + stockCodes.get (index - 1))
                  words += ("visitWebsite:" + visitWebsite )

                }
              }
            }
          }
        }

      } else if (s.startsWith ("hash:visit:")) {

        val keys: Array[String] = s.split ("_kunyan_")
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
      case e:Exception =>
        PrismLogger.error(s"mapFunct error:$s")
    }
    words
  }

  val sparkConf = new SparkConf()
    .setAppName("SearchAndVisitScheduler")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "2000")
    .set("spark.driver.allowMultipleContexts", "true")

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)


  def getTableData(tableName:String, connection:String): Unit = {

    val properties = new Properties()
    properties.setProperty("driver", "com.mysql.jdbc.Driver")
    sqlContext.read.jdbc(connection,tableName,properties).foreach(row => {
      stockCodes.add(row(0).toString)
      nameUrls.+=(row(2).toString)
      jianPins.+=(row(3).toString)
      quanPins.+=(row(4).toString)
    })
  }

  def main(args: Array[String]) {

    if (args.length < 1) {

      System.err.println("Usage: shDataFile,jsDataFile,zjDataFile, date, dir, ruleCountDir,con")
      System.exit(1)

    }

    try {

      val Array(shDataFile,jsDataFile,zjDataFile, date, dir, ruleCountDir, con) = args

      val fileJsdx = sc.textFile(jsDataFile)
      val fileZjdx = sc.textFile(zjDataFile)
      val fileShdx = sc.textFile(shDataFile)
      val file = fileJsdx.++(fileShdx).++(fileZjdx)

      val dbInfo = sc.textFile(con).collect()(0)


      val map = Map("all" -> file, "jsdx" -> fileJsdx, "zjdx" -> fileZjdx, "shdx" -> fileShdx)

      // 写入本地文件中(备份用)
      val nowDate: String = date

      getTableData("stock_info", dbInfo)

      map.foreach(x => {

        val key = x._1
        val dataValue = x._2

        var lines:RDD[String] = null

        if(date >= "2016-06-13" ) {
          lines = dataValue.flatMap(flatMapFunLater)
        } else {
          lines = dataValue.flatMap(flatMapFun)
        }


        val data = lines.flatMap(mapFunct).map((_,1)).reduceByKey(_+_)

        if(key == "all") {

          val result = data.filter(x => x._1.startsWith("hash:")).map { line =>
            val arr = line._1.split("_kunyan_")
            val stock = arr(1)
            val hour = arr(0).split(":")(2)
            val typeString = arr(0).split(":")(1)

            (typeString,(stock,hour,line._2))

          }

          val fileSearchData = result.filter(_._2._2.contains(nowDate)).filter(_._1 == "search").map( x => (x._2._1, x._2._2 , x._2._3.toString))

          val fileVisitData = result.filter(_._2._2.contains(nowDate)).filter(_._1 == "visit").map( x => (x._2._1 , x._2._2 , x._2._3.toString))

          FileUtil.writeToCSV(dir +"/" + date + "_search",fileSearchData.collect())
          FileUtil.writeToCSV(dir + "/" + date +  "_visit",fileVisitData.collect)

        } else {

          val result = data.filter(x => x._1.startsWith("visitWebsite")).map { line =>

            val arr = line._1.split(":")
            val visitWebSite = if(arr.length > 1 && arr(1).forall(Character.isDigit)) arr(1) else ""

            (visitWebSite,line._2.toString)

          }.filter(x => x._1 != "")

          FileUtil.writeRuleCountToCSV(ruleCountDir + "/" + key  + "/" + date + "_count", result.collect())

        }
      })

    } catch {
      case e:Exception =>
        PrismLogger.error(" search and visit over !")
    }

    sc.stop()

  }

}
