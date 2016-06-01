package com.kunyan.data.scheduler

import java.text.SimpleDateFormat
import java.util
import java.util.regex.Pattern
import java.util.{Date, Properties}

import com.kunyan.data.database.MysqlOperation
import com.kunyan.data.log.PrismLogger
import com.kunyan.data.util.FileUtil
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Created by C.J.YOU on 2016/5/27.
  * 访问和搜索 主程序入口
  */
object SearchAndVisitScheduler {

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
          lineList += ("hash:visit:" + hour + "," + stockCode)
        } else if (visitWebsite.charAt (0) >= '6' && visitWebsite.charAt (0) <= '9') {
          lineList += ("hash:search:" + hour + "," + stockCode)
        }
      }
    } catch {
      case e:Exception => println(s"flatMapFun error:$line")
    }
    lineList
  }
  def mapFunct(s:String):mutable.MutableList[String] ={
    var words: mutable.MutableList[String] = new mutable.MutableList[String]()
    try {
      if (s.startsWith ("hash:search:")) {
        val keys: Array[String] = s.split (",")
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
                  words += ("hash:search:" + hours + "," + stockCode)
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
                  words += (keys (0) + "," + stockCodes.get (index - 1))
                }
              }
            }

          } else if ((firstChar >= 'A' && firstChar <= 'Z') || (firstChar >= 'a' && firstChar <= 'z')) {
            keyWord = keyWord.toLowerCase ()
            var index: Int = 0
            for (jianPin <- jianPins) {
              index += 1
              if (jianPin.contains (keyWord)) {
                words += (keys (0) + "," + stockCodes.get (index - 1))
              }
            }
            if (index == 0) {
              for (quanPin <- quanPins) {
                index += 1
                if (quanPin.contains (keyWord)) {
                  words += (keys (0) + "," + stockCodes.get (index - 1))
                }
              }
            }
          }
        }
      } else if (s.startsWith ("hash:visit:")) {
        val keys: Array[String] = s.split (",")
        if (keys.length < 2) {
          return words
        } else {
          val keyWord = keys (1)
          if (stockCodes.contains (keyWord)) {
            words += s
          }
        }
      }
    } catch {
      case e:Exception => println(s"mapFunct error:$s")
    }
    words
  }

  def getTableData(tableName:String): Unit = {
    sqlContext.read.jdbc(MysqlOperation.connection,tableName,MysqlOperation.properties).foreach(row =>{
      stockCodes.add(row(0).toString)
    })
  }

  val sparkConf = new SparkConf()
    .setAppName("SearchAndVisitScheduler")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "2000")
    .set("spark.driver.allowMultipleContexts", "true")
    .setMaster("local")

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: LoadData dataFile,typeS,month")
      System.exit(1)
    }
    try {

      val Array(dataFile, connection, typeS, month) = args
      val file = sc.textFile(dataFile)

      /*
      写入本地文件中
      val nowDate = args(2)
      getTableData("stock_info")

      val lines = file.flatMap(flatMapFun)
      System.out.println("flatmap:start")
      val result = lines.flatMap(mapFunct).map((_,1)).reduceByKey(_+_)
        .map { line =>
          val arr = line._1.split(",")
          val stock = arr(1)
          val hour = arr(0).split(":")(2)
          val typeString = arr(0).split(":")(1)
          (typeString,(stock,hour,line._2))
        }.cache()

      val fileSearchData = result.filter(_._1 == "search").map( x => x._2._1 + "\t" + x._2._2 + "\t" + x._2._3)
      val fileVisitData = result.filter(_._1 == "visit").map( x => x._2._1 + "\t" + x._2._2 + "\t" + x._2._3)

      FileUtil.writeToFile(args(1)+"_search",fileSearchData.collect)
      FileUtil.writeToFile(args(1)+"_visit",fileVisitData.collect) */

      // 写入92 mysql数据库中

      val lines = file.map { x =>
        val arr = x.split("\t")
        val stock = arr(0)
        val hour = arr(1)
        val c = arr(2)
        ((stock,hour),c.toInt)
      }.reduceByKey(_ + _)

      val data = lines.filter(_._1._2.contains("-"+ month +"-")).map(x => Row(x._1._1,x._1._2,x._2))

      val schema =
        StructType(
          StructField("v_stock",StringType,nullable = false)::
            StructField("v_hour",StringType,nullable = false)::StructField("i_frequency",IntegerType,nullable = true)::Nil)

      if(typeS == "search") {
        sqlContext.createDataFrame(data, schema).write.mode(SaveMode.Append).jdbc(connection,MysqlOperation.searchTable , MysqlOperation.properties)
      }
      else if(typeS == "visit") {
        sqlContext.createDataFrame(data, schema).write.mode(SaveMode.Append).jdbc(connection,MysqlOperation.visitTable , MysqlOperation.properties)
      }
      else if(typeS == "follow"){
        sqlContext.createDataFrame(data, schema).write.mode(SaveMode.Append).jdbc(connection,MysqlOperation.followTable , MysqlOperation.properties)
      }
    } catch {
      case e:Exception =>
        PrismLogger.info(" search and visit over !")
    }finally {

    }
    sc.stop()
  }

}
