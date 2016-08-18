package scheduler

import java.math.BigInteger
import java.text.SimpleDateFormat
import java.util
import java.util.Properties
import java.util.regex.Pattern

import _root_.util.RedisUtil
import log.PrismLogger
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import searchandvisit.SearchAndVisit

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/2/4.
  * 电信数据解析主程序
  */
object Scheduler {

  val RedisInfoList: mutable.MutableList[String] = mutable.MutableList[String]()
  var confInfoMap = new mutable.HashMap[String, String]()
  var stockCodes:java.util.List[String] = new util.ArrayList[String]()
  var nameUrls = mutable.MutableList[String]()
  var jianPins = mutable.MutableList[String]()
  var quanPins = mutable.MutableList[String]()

  var userMap = new mutable.HashMap[String, ListBuffer[String]]
  var stockCodeMap = new mutable.HashMap[String, ListBuffer[String]]

  val sparkConf = new SparkConf()
    .setAppName("LoadData_Analysis")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "2000")

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  /**
    * 从数据库读取股票初始化数据
    *
    * @param tableName 表名
    * @param connection 连接
    */
  def getTableData(tableName:String,connection:String): Unit = {

    val properties = new Properties()
    properties.setProperty("driver", "com.mysql.jdbc.Driver")
    sqlContext.read.jdbc(connection,tableName,properties).foreach( row => {
      stockCodes.add(row(0).toString)
      nameUrls.+=(row(2).toString)
      jianPins.+=(row(3).toString)
      quanPins.+=(row(4).toString)
    })

  }

  /**
    * 从redis读取股票初始化数据
    */
  def initRedisData(): Unit = {

    val jedis = RedisUtil.getRedis(confInfoMap("ip"),confInfoMap("port"),confInfoMap("auth"),confInfoMap("database"))

    try {

      PrismLogger.warn("redis connected")
      stockCodes = jedis.lrange("stock:list", 0, -1)
      val iterator: util.Iterator[String] = stockCodes.iterator()

      while (iterator.hasNext) {

        val stockCode = iterator.next()
        nameUrls.+=(jedis.get("stock:" + stockCode + ":nameurl"))
        jianPins.+=(jedis.get("stock:" + stockCode + ":jianpin"))
        quanPins.+=(jedis.get("stock:" + stockCode + ":quanpin"))

      }

    } finally {
      jedis.close()
    }

  }

  def getTime(timeStamp: String): String = {

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val bigInt: BigInteger = new BigInteger(timeStamp)
    val date: String = sdf.format(bigInt)

    date

  }

  def getConfInfo(line :String):(String,String) = {

    val  lineSplits: Array[String] = line.split("=")
    val  attr = lineSplits(0)
    val confValue = lineSplits(1)

    (attr,confValue)

  }

  /**
    * 判断原始数据是搜索还是查看
    * @param line 原始数据
    * @return 区分后的数据集合
    */
  def flatMapFun(line: String): mutable.MutableList[String] = {

    val lineList: mutable.MutableList[String] = mutable.MutableList[String]()
    val lineSplits: Array[String] = line.split("\t")

    if (lineSplits.length < 3) {
      lineList +="not right"
    }

    val stockCode = lineSplits(0)
    val timeStamp = lineSplits(1)
    val visitWebsite = lineSplits(2)
    val hour = getTime(timeStamp)

    if ((!" ".equals (stockCode)) && timeStamp != null && visitWebsite != null) {

      if(visitWebsite forall Character.isDigit) {

        if (visitWebsite.toInt >= 42  && visitWebsite.toInt <= 95) {
          lineList += ("hash:visit:" + hour + "," + stockCode)
        } else if (visitWebsite.toInt >= 0 && visitWebsite.toInt <= 41) {
          lineList += ("hash:search:" + hour + "," + stockCode)
        }
      }
    }

    lineList

  }

  /**
    * 匹配原始数据
    *
    * @param s 原始数据
    * @return 返回匹配后的数据集合
    */
  def mapFunction(s:String):mutable.MutableList[String] = {

    var words: mutable.MutableList[String] = new mutable.MutableList[String]()

    if(s.startsWith("hash:search:")) {

      val keys:Array[String]  = s.split(",")

      if(keys.length < 2) {
        return words
      } else {

        var keyWord = keys(1)
        val hours = keys(0).split(":")(2)

        if(keyWord.length < 4) {
          return words
        }

        val firstChar = keyWord.charAt(0)

        if(firstChar >= '0' && firstChar <= '9') {

          if(keyWord.length < 6) {
            return words
          } else {

            val iterator:util.Iterator[String] = stockCodes.iterator()

            while(iterator.hasNext) {
              val stockCode = iterator.next()

              if(stockCode.contains(keyWord)) {

                words +=("hash:search:" + hours + "," + stockCode)
                words +=("search:count:" + hours)
                words +=("search:" + stockCode + ":" + hours)

              }
            }
          }

        } else if(firstChar == '%') {

          val pattern:Pattern = Pattern.compile("%.*%.*%.*%.*%.*%.*%.*%")

          if(! pattern.matcher(keyWord).find()) {
            return words
          } else {

            keyWord = keyWord.toUpperCase()
            var index:Int = 0

            for(nameUrl <- nameUrls) {

              index += 1

              if(nameUrl.contains(keyWord)) {

                words +=(keys(0) + "," + stockCodes.get(index-1))
                words +=("search:count:" + hours)
                words +=("search:" + stockCodes.get(index-1) + ":" + hours)

              }
            }
          }

        } else if((firstChar >= 'A' && firstChar <= 'Z') || (firstChar >= 'a' && firstChar <= 'z')) {

          keyWord = keyWord.toLowerCase()
          var index:Int = 0

          for(jianPin <- jianPins) {

            index += 1

            if(jianPin.contains(keyWord)) {

              words += (keys(0)+","+stockCodes.get(index-1))
              words += ("search:count:" + hours)
              words += ("search:" + stockCodes.get(index-1) + ":" + hours)

            }
          }

          if(index == 0) {

            for(quanPin <- quanPins){

              index += 1

              if(quanPin.contains(keyWord)) {

                words += (keys(0) + "," + stockCodes.get(index - 1))
                words += ("search:count:" + hours)
                words += ("search:" + stockCodes.get(index - 1) + ":" + hours)

              }
            }
          }
        }
      }

    } else if(s.startsWith("hash:visit:")) {

      val keys:Array[String]  = s.split(",")

      if(keys.length < 2) {
        words += "no"
      } else {

        val keyWord = keys(1)
        val hours = keys(0).split(":")(2)

        if(stockCodes.contains(keyWord)) {

          words += s
          words += ("visit:count:" + hours)
          words += ("visit:" + keyWord + ":" + hours)

        }
      }
    }

    words

  }

  def main(args: Array[String]) {

    if (args.length < 2) {

      System.err.println("Usage: LoadData <redis Conf file> <data Files>")
      System.exit(1)

    }

    val confRead = sc.textFile(args(0))
    val alertList = confRead.map(getConfInfo)

    val file = sc.textFile(args(1))
    val fileJsdx = sc.textFile(args(2))
    val fileZjdx = sc.textFile(args(3))
    val fileShdx = sc.textFile(args(4))

    val map = Map("all" -> file, "jsdx" -> fileJsdx, "zjdx" -> fileZjdx, "shdx" -> fileShdx)

    val dbInfo = sc.textFile(args(5)).collect()(0)

    alertList.collect().foreach {
      case (attr,value)  =>
        if (attr == "ip") confInfoMap.+=(("ip", value))
        if (attr == "port") confInfoMap.+=(("port", value))
        if (attr == "auth") if (value != null) confInfoMap.+=(("auth", value)) else confInfoMap.+=(("auth", null))
        if (attr == "database") confInfoMap.+=(("database", value))
        if (attr == "urlString") confInfoMap.+=(("urlString", value))
        if (attr == "SV_DELAYHOUR") confInfoMap.+=(("SV_DELAYHOUR", value))
        if (attr == "F_DELAYHOUR") confInfoMap.+=(("F_DELAYHOUR", value))
        if (attr == "FOLLOW_NUMBER") confInfoMap.+=(("FOLLOW_NUMBER", value))
        if (attr == "SEARCH_NUMBER") confInfoMap.+=(("SEARCH_NUMBER", value))
        if (attr == "VISIT_NUMBER") confInfoMap.+=(("VISIT_NUMBER", value))
    }


    val jedis = RedisUtil.getRedis(confInfoMap("ip"),confInfoMap("port"),confInfoMap("auth"),confInfoMap("database"))

    getTableData("stock_info",dbInfo)

    try {

      map.foreach {x =>

        val counts = x._2.flatMap(flatMapFun).flatMap(mapFunction).map((_,1)).reduceByKey(_+_).collect()
        SearchAndVisit.writeDataToRedis(jedis,counts,x._1)

      }

    } catch {

      case e:Exception =>
        PrismLogger.info(" VS Operation Exception" + e.printStackTrace())
        PrismLogger.exception(e)
        System.exit(-1)

    } finally {
      jedis.close()
    }
    sc.stop()

  }
}
