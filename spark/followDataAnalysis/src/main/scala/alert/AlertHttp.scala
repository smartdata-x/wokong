package alert

import java.util
import java.util.{List, Calendar}
import _root_.util.{FileUtil, TimeUtil}
import config.FileConfig

import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/1/19.
  */
object AlertHttp extends Http {

  var FOLLOW_NUMBER = 400
  var SEARCH_NUMBER = 600
  var VISIT_NUMBER = 2000
  var SV_DELAYHOUR = -5
  var F_DELAYHOUR = -1
  var urlString = "http://120.55.189.211/cgi-bin/northsea/prsim/subscribe/1/stock_notice.fcgi"

  var maxFollow = 0
  var maxSearch = 0
  var maxVisit = 0

  val followList = new mutable.MutableList[String]
  val searchList = new mutable.MutableList[String]
  val visitList = new mutable.MutableList[String]

  val parse = "prase"

  var SEND_NUMBER = 0

  override def get(strUrl: String, parameters: mutable.HashMap[String, String], parse: (String)): Unit = super.get(strUrl, parameters, parse)

  def init(confParam: mutable.HashMap[String, String]): Unit ={

    FOLLOW_NUMBER =confParam("FOLLOW_NUMBER").toInt
    SEARCH_NUMBER =confParam("SEARCH_NUMBER").toInt
    VISIT_NUMBER =confParam("VISIT_NUMBER").toInt
    SV_DELAYHOUR =confParam("SV_DELAYHOUR").toInt
    F_DELAYHOUR =confParam("F_DELAYHOUR").toInt
    urlString = confParam("urlString")
  }

  def isFollowAlertStockCode(jedis:Jedis,stockCode:String, beginHour:String,endHour:String):Boolean ={
    val s1 = jedis.get("follow:"+stockCode+":"+beginHour)
    val s2 = jedis.get("follow:"+stockCode+":"+endHour)
    if(s1 != null && s2 != null){
      val beginValue = Integer.parseInt(s1)
      val endValue = Integer.parseInt(s2)
      followList.+=(stockCode+"\t"+ (endValue - beginValue)+"\t"+endHour)
      if(endValue - beginValue > maxFollow) {
        maxFollow = endValue - beginValue
      }
      if(endValue - beginValue > FOLLOW_NUMBER){
         true
      }else{
        false
      }
    }else {
      false
    }
  }

  def isSearchAlertStockCode(jedis:Jedis,stockCode:String, beginHour:String,endHour:String):Boolean={
      val s1 = jedis.get("search:"+stockCode+":"+beginHour)
      val s2 = jedis.get("search:"+stockCode+":"+endHour)
      if(s1 != null && s2 != null){
        val beginValue = Integer.parseInt(s1)
        val endValue = Integer.parseInt(s2)
        searchList.+=(stockCode+"\t"+ (endValue - beginValue))
        if(endValue - beginValue > maxSearch) {
          maxSearch =endValue - beginValue
        }
        if(endValue - beginValue > SEARCH_NUMBER){
          true
        }else{
          false
        }
      }else {
        false
      }
  }

  def isVisitAlertStockCode(jedis:Jedis,stockCode:String, beginHour:String,endHour:String ):Boolean={
    val s1 = jedis.get("visit:"+stockCode+":"+beginHour)
    val s2 = jedis.get("visit:"+stockCode+":"+endHour)
    if(s1 != null && s2 != null){
      val beginValue = Integer.parseInt(s1)
      val endValue = Integer.parseInt(s2)
      visitList.+=(stockCode+"\t"+ (endValue - beginValue))
      if(endValue - beginValue > maxVisit) {
        maxVisit =endValue - beginValue
      }
      if(endValue - beginValue > VISIT_NUMBER){
         true
      }else{
        false
      }
    }else {
      false
    }
  }

  /** 获取需要报警的 股票代码并实时报警 (SEARCH AND VISIT) */
  def alertSV(jedis:Jedis,stockCodes: List[String]): Unit ={
    val calendarNew = Calendar.getInstance()
    System.out.println("alertSV currtentHour:"+TimeUtil.getTime(String.valueOf(calendarNew.getTimeInMillis)))
    calendarNew.add(Calendar.HOUR,SV_DELAYHOUR)
    val startHour = TimeUtil.getTime(String.valueOf(calendarNew.getTimeInMillis))
    System.out.println("alertSV startHour:"+startHour)
    calendarNew.add(Calendar.HOUR, 1)
    val endHour = TimeUtil.getTime(String.valueOf(calendarNew.getTimeInMillis))
    System.out.println("alertSV endHour:"+endHour)
    val iterator:util.Iterator[String] = stockCodes.iterator()
    while(iterator.hasNext){
      val stockCode = iterator.next()
      if(isSearchAlertStockCode(jedis,stockCode, startHour, endHour)){
        requestOneByOne(stockCode,2)
        SEND_NUMBER = SEND_NUMBER +1
      }
      if(isVisitAlertStockCode(jedis,stockCode, startHour, endHour)){
        requestOneByOne(stockCode,3)
        SEND_NUMBER = SEND_NUMBER +1
      }
    }
    /** 记录 alert的历史数据到本地文件夹 */
    val dir = FileConfig.ALERT_SV_DATA_FILE + "/" +TimeUtil.getDate(System.currentTimeMillis().toString)
    FileUtil.mkDir(dir)
    recordFollowAlertData(dir,2,searchList,startHour,endHour)
    recordFollowAlertData(dir,3,visitList,startHour,endHour)
  }

  /** 获取需要报警的 股票代码并实时报警 (SEARCH AND VISIT) */
  def alertF(jedis:Jedis,stockCodes: List[String]): Unit ={
    val calendarNew = Calendar.getInstance()
    System.out.println("alertF currtentHour:"+TimeUtil.getTime(String.valueOf(calendarNew.getTimeInMillis)))
    calendarNew.add(Calendar.HOUR,F_DELAYHOUR)
    val startHour = TimeUtil.getTime(String.valueOf(calendarNew.getTimeInMillis))
    System.out.println("alertF startHour:"+startHour)
    calendarNew.add(Calendar.HOUR, 1)
    val endHour = TimeUtil.getTime(String.valueOf(calendarNew.getTimeInMillis))
    System.out.println("alertF endHour:"+endHour)
    val iterator:util.Iterator[String] = stockCodes.iterator()
    while(iterator.hasNext){
      val stockCode = iterator.next()
      if(isFollowAlertStockCode(jedis,stockCode, startHour, endHour)){
        requestOneByOne(stockCode,1)
        SEND_NUMBER = SEND_NUMBER +1
      }
    }
    /** 记录 alert的历史数据到本地文件夹 */
    val dir = FileConfig.ALERT_FOLLOW_DATA_FILE + "/" +TimeUtil.getDate(System.currentTimeMillis().toString)
    FileUtil.mkDir(dir)
    recordFollowAlertData(dir,1,followList,startHour,endHour)
  }

  /**
    *记录 alert的历史数据
    */
  def recordFollowAlertData(dir:String,msgType:Int,list:mutable.MutableList[String],startHour:String,endHour:String): Unit ={
    FileUtil.mkDir(dir +"/follow")
    FileUtil.mkDir(dir +"/search")
    FileUtil.mkDir(dir +"/visit")
    if(msgType == 1){
      FileUtil.createFile(dir +"/follow"+"/"+startHour+"->"+endHour,list.toSeq)
    }
    if(msgType == 2){
      FileUtil.createFile(dir +"/search"+"/"+startHour+"->"+endHour,list.toSeq)
    }
    if(msgType == 3){
      FileUtil.createFile(dir +"/visit"+"/"+startHour+"->"+endHour,list.toSeq)
    }
  }

  /** 数据请求 one by one
    */
  def requestOneByOne(msg:String,msgType: Int): Unit ={
    try{
      val paraMap = new scala.collection.mutable.HashMap[String,String]
      val sendMsg = msg
      paraMap.+=(("stock_code",sendMsg+"x"))
      if(msgType == 1){
        paraMap.+=(("stock_type","follow"))
        get(urlString,paraMap,parse)
      }
      if(msgType == 2){
        paraMap.+=(("stock_type","search"))
        get(urlString,paraMap,parse)
      }
      if(msgType == 3){
        paraMap.+=(("stock_type","visit"))
        get(urlString,paraMap,parse)
      }
    }catch {
      case e:Exception =>
        println("requestOneByOne exception")
    }

  }

  /** 发送request的请求 */
  def request(msg:mutable.MutableList[String],msgType: Int): Unit ={
    if(msg.nonEmpty){
      var sendMsg  = new String
      for(msgList <- msg){
        sendMsg += msgList
      }
      val paraMap = new scala.collection.mutable.HashMap[String,String]
      paraMap.+=(("stock_code",sendMsg))
      if(msgType == 1){
        paraMap.+=(("stock_type","follow"))
        get(urlString,paraMap,parse)
      }
      if(msgType == 2){
        paraMap.+=(("stock_type","search"))
        get(urlString,paraMap,parse)
      }
      if(msgType == 3){
        paraMap.+=(("stock_type","visit"))
        get(urlString,paraMap,parse)
      }
    }
  }
}
