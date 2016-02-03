/**
  * @author C.J.YOU
  * 2015年12月11日

  * Copyright (c)  by ShangHai KunYan Data Service Co. Ltd ..  All rights reserved.

  * By obtaining, using, and/or copying this software and/or its
  * associated documentation, you agree that you have read, understood,

  * and will comply with the following terms and conditions:

  * Permission to use, copy, modify, and distribute this software and
  * its associated documentation for any purpose and without fee is
  * hereby granted, provided that the above copyright notice appears in
  * all copies, and that both that copyright notice and this permission
  * notice appear in supporting documentation, and that the name of
  * ShangHai KunYan Data Service Co. Ltd . or the author
  * not be used in advertising or publicity
  * pertaining to distribution of the software without specific, written
  * prior permission.
  *
  */
package lengjing

import java.math.BigInteger
import java.text.SimpleDateFormat
import java.util
import java.util.regex.Pattern

import _root_.util.RedisUtil
import alert.AlertHttp
import follow.FollowStock
import message.SendMessage

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object LoadData {

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
  //val setFollowCount ="set:follow:count:"
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
  def mapFunct(s:String):mutable.MutableList[String] ={
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
  def main(args: Array[String]): Unit={
    if (args.length < 2) {
      System.err.println("Usage: LoadDataScala <redis Conf file> <data Files>")
      System.exit(1)
    }
    val sparkConf = new SparkConf()
      .setAppName("LoadDataScala")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000")
      // .setMaster("local")
    val sc = new SparkContext(sparkConf)
    val confRead = sc.textFile(args(0))
    val alertList = confRead.map(getConfInfo)
    val file = sc.textFile(args(1))

    // info.foreach(print(_))  //(ip,222.73.34.96)(port,6390)(auth,7ifW4i@M)(database,0)
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

    /** connection redis server */
    val jedis = RedisUtil.getRedis(confInfoMap("ip"),confInfoMap("port"),confInfoMap("auth"),confInfoMap("database"))
    val p = jedis.pipelined()
    /** init data from redis such as stockCodes... */
    initRedisData()
    val lines = file.flatMap(flatMapFun)
    System.out.println("flatmap:start")
    val flatmap =lines.flatMap(mapFunct).map((_,1)).reduceByKey(_+_)
      /** write data to redis */
    val counts  = flatmap.collect()
    try {
      counts.foreach(line => {
        if (line._1.startsWith("hash:")) {
          val firstKeys: Array[String] = line._1.split(",")
          val keys: Array[String] = firstKeys(0).split(":")
          val outKey: String = keys(1) + ":" + keys(2)
          val dayKey = keys(2).split(" ")
          p.hincrBy(outKey, firstKeys(1),line._2)
          p.expire(outKey, 50 * 60 * 60)
          /* 添加当天的set list */
          p.zincrby("set:" + keys(1) + ":" + dayKey(0), line._2, firstKeys(1))
          p.expire("set:" + keys(1) + ":" + dayKey(0), 50 * 60 * 60)
          /* 添加当天的set count */
          p.incrBy("set:"+ keys(1)+":count:" + dayKey(0), line._2)
          p.expire("set:"+ keys(1)+":count:" + dayKey(0), 50 * 60 * 60)
        } else {
          p.incrBy(line._1, line._2)
          p.expire(line._1, 50 * 60 * 60)
        }
      })
//      /** 获取 hbase的follow 信息 **/
//      val list = FileUtil.readFile(FileConfig.ROOT_DIR + FileConfig.HBASE_CONF_FILENAME)
//      // val list = FileUtil.readFile(FileConfig.TEST_ROOT + FileConfig.TEST_HBASE_CONF_FILENAME)
//      var timeRangeMap = new mutable.HashMap[String,String]()
//      list.foreach(x =>{
//        val split = x.split("=")
//        println(split(0)+"time split stamp--->>>"+split(1))
//        timeRangeMap.+=(split(0) -> split(1))
//      })
//      println("timeRange>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>:"+timeRangeMap("last_timeStamp")+":")
//
//      TableHbase.getStockCodesFromHbase(sc,timeRangeMap("last_timeStamp"))
//      System.out.println("---Follow stockCodeMap size:----"+TableHbase.getStockCodeMap.size)
//      System.out.println("---Follow userMap size:----"+TableHbase.getUserMap.size)
//      FollowStock.writeToRedis(jedis,stockCodes,TableHbase.getStockCodeMap)
//      System.out.println("--- Write timeStamp for hbase----<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< ----")
//      FileUtil.createFile(FileConfig.ROOT_DIR + FileConfig.HBASE_CONF_FILENAME,"last_timeStamp="+System.currentTimeMillis())
      // FileUtil.createFile(FileConfig.TEST_ROOT + FileConfig.TEST_HBASE_CONF_FILENAME,"last_timeStamp="+System.currentTimeMillis())

    } catch {
      case e:Exception =>
        p.clear()
        p.close()
        println(" VS Operation Exception"+e.printStackTrace())
        SendMessage.sendMessage(1,"电信平台计算", "数据解析操作异常")
    }
    p.syncAndReturnAll()
    try{
      AlertHttp.init(confInfoMap)
      AlertHttp.alertSV(jedis, stockCodes)
      println("alertSV followList number size--"+AlertHttp.followList.size)
      println("alertSV searchList number size--"+AlertHttp.searchList.size)
      println("alertSV visitList number size--"+AlertHttp.visitList.size)
    }catch {
      case e:Exception =>
        println("alertSV Exception")
        SendMessage.sendMessage(1,"电信平台计算", "搜索和查看报警操作异常")
    }
    try{
      AlertHttp.alertF(jedis, stockCodes)
      println("----------alertF followList number size--"+AlertHttp.followList.size)
      println("----------alertF searchList number size--"+AlertHttp.searchList.size)
      println("-----------alertF visitList number size--"+AlertHttp.visitList.size)
      System.out.println("---Alert Over----")
      println("Total Alert number--"+AlertHttp.SEND_NUMBER)
    }catch {
      case e:Exception =>
        println("alertF Exception")
        SendMessage.sendMessage(1,"电信平台计算", "关注报警操作异常")
    }
    try{
    /** 添加 add follow 数据到 redis **/
      FollowStock.writeAddFollowDataToRedis(jedis,AlertHttp.followList)
      System.out.println("---add Follow Alert to Redis F---p.syncAndReturnAll()----")
    }catch {
      case e:Exception =>
        println("add Follow Alert to Redis Exception")
        SendMessage.sendMessage(1,"电信平台计算", "写入关注数据操作异常")
    }finally {
      /** 关闭连接词的实例 **/
      jedis.close()
      sc.stop()
      System.exit(-1)
    }

  }
}
