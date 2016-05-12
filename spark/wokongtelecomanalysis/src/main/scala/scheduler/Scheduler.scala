package scheduler

import java.math.BigInteger
import java.text.SimpleDateFormat
import java.util
import java.util.regex.Pattern

import _root_.util.RedisUtil
import log.PrismLogger
import message.SendMessage
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
  var confInfoMap = new mutable.HashMap[String,String]()
  var stockCodes:java.util.List[String] = new util.ArrayList[String]()
  var nameUrls = mutable.MutableList[String]()
  var jianPins = mutable.MutableList[String]()
  var quanPins = mutable.MutableList[String]()

  var userMap = new mutable.HashMap[String, ListBuffer[String]]
  var stockCodeMap = new mutable.HashMap[String, ListBuffer[String]]

  val  setSearchCount ="set:search:count:"
  val  setVisitCount ="set:visit:count:"
  val setSearch ="set:search:"
  val setVisit ="set:visit:"

  def initRedisData(): Unit ={
    val jedis2 = RedisUtil.getRedis(confInfoMap("ip"),confInfoMap("port"),confInfoMap("auth"),confInfoMap("database"))
    System.out.println("redis connected")
    stockCodes = jedis2.lrange("stock:list", 0, -1)
    val iterator:util.Iterator[String] = stockCodes.iterator()
    while(iterator.hasNext){
      val stockCode = iterator.next()
      nameUrls.+=(jedis2.get("stock:"+stockCode+":nameurl"))
      jianPins.+=(jedis2.get("stock:"+stockCode+":jianpin"))
      quanPins.+=(jedis2.get("stock:"+stockCode+":quanpin"))
    }
    jedis2.close()

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
    /** visit */
    if ((!" ".equals(stockCode)) && timeStamp != null && visitWebsite != null) {
      if (visitWebsite.charAt(0) >= '0' && visitWebsite.charAt(0) <= '5') {
        lineList += ("hash:visit:" + hour + "," + stockCode)
      } else if (visitWebsite.charAt(0) >= '6' && visitWebsite.charAt(0) <= '9') {
        lineList += ("hash:search:" + hour + "," + stockCode)
      }
    }
    lineList
  }
  def mapFunction(s:String):mutable.MutableList[String] ={
    var words: mutable.MutableList[String] = new mutable.MutableList[String]()
    if(s.startsWith("hash:search:")){
      val keys:Array[String]  = s.split(",")
      if(keys.length < 2){

      }else{
        var keyWord = keys(1)
        val hours = keys(0).split(":")(2)
        if(keyWord.length < 4){
          return words
        }
        val firstChar = keyWord.charAt(0)
        if(firstChar >= '0' && firstChar <= '9'){
          if(keyWord.length < 6) {
            return words
          }else{
            val iterator:util.Iterator[String] = stockCodes.iterator()
            while(iterator.hasNext){
              val stockCode = iterator.next()
              if(stockCode.contains(keyWord)){
                words +=("hash:search:"+hours+","+stockCode)
                words +=("search:count:"+hours)
                words +=("search:"+stockCode+":"+hours)
              }
            }
          }
        }else if(firstChar == '%'){
          val pattern:Pattern = Pattern.compile("%.*%.*%.*%.*%.*%.*%.*%")
          if(! pattern.matcher(keyWord).find()){
            return words
          }else{
            keyWord = keyWord.toUpperCase()
            var index:Int = 0
            for(nameUrl <- nameUrls){
              index += 1
              if(nameUrl.contains(keyWord)){
                words +=(keys(0)+","+stockCodes.get(index-1))
                words +=("search:count:"+hours)
                words +=("search:"+stockCodes.get(index-1)+":"+hours)
              }
            }
          }

        }else if((firstChar >= 'A' && firstChar <= 'Z') || (firstChar >= 'a' && firstChar <= 'z')){
          keyWord = keyWord.toLowerCase()
          var index:Int = 0
          for(jianPin <- jianPins){
            index += 1
            if(jianPin.contains(keyWord)){
              words += (keys(0)+","+stockCodes.get(index-1))
              words += ("search:count:"+hours)
              words += ("search:"+stockCodes.get(index-1)+":"+hours)
            }
          }
          if(index == 0){
            for(quanPin <- quanPins){
              index += 1
              if(quanPin.contains(keyWord)){
                words += (keys(0)+","+stockCodes.get(index-1))
                words += ("search:count:"+hours)
                words += ("search:"+stockCodes.get(index-1)+":"+hours)
              }
            }
          }
        }
      }
    }else if(s.startsWith("hash:visit:")){
      val keys:Array[String]  = s.split(",")
      if(keys.length < 2){
        words +="no"
      }else{
        val keyWord = keys(1)
        val hours = keys(0).split(":")(2)
        if(stockCodes.contains(keyWord)){
          words += s
          words += ("visit:count:"+hours)
          words += ("visit:"+keyWord+":"+hours)
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
    val sparkConf = new SparkConf()
      .setAppName("LoadData_Analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)
    val confRead = sc.textFile(args(0))
    val alertList = confRead.map(getConfInfo)
    val file = sc.textFile(args(1))

    // (ip,222.73.34.96)(port,6390)(auth,7ifW4i@M)(database,0)
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

    /**
      * connection redis server
      */
    val jedis = RedisUtil.getRedis(confInfoMap("ip"),confInfoMap("port"),confInfoMap("auth"),confInfoMap("database"))
    /**
      * init data from redis such as stockCodes
      */
    initRedisData()
    val lines = file.flatMap(flatMapFun)
    System.out.println("flatmap:start")
    val flatmap =lines.flatMap(mapFunction).map((_,1)).reduceByKey(_+_)
    /**
      * write data to redis
      */
    val counts  = flatmap.collect()
    try {
      SearchAndVisit.writeDataToRedis(jedis,counts)
    } catch {
      case e:Exception =>
        PrismLogger.info(" VS Operation Exception"+e.printStackTrace())
        SendMessage.sendMessage(1,"电信平台计算", "数据解析操作异常")
        PrismLogger.exception(e)
        System.exit(-1)
    }finally {
      jedis.close()
    }
    sc.stop()
    System.exit(0)
  }
}
