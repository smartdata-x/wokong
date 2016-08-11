package scheduler

import java.util

import _root_.util.RedisUtil
import alert.AlertHttp
import follow.FollowStock
import log.PrismLogger
import message.SendMessage
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable


/**
  * Created by C.J.YOU on 2016/2/4.
  * 热度报警主程序
  */
object Scheduler {
    var confInfoMap = new mutable.HashMap[String,String]()
    var stockCodes:java.util.List[String] = new util.ArrayList[String]()
    var nameUrls = mutable.MutableList[String]()
    var jianPins = mutable.MutableList[String]()
    var quanPins = mutable.MutableList[String]()
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

    def getConfInfo(line :String):(String,String) = {
      val  lineSplits: Array[String] = line.split("=")
      val  attr = lineSplits(0)
      val confValue = lineSplits(1)
      (attr,confValue)
    }

    def main(args: Array[String]) {
      if (args.length < 1) {
        System.err.println("Usage: LoadData <redis Conf file>")
        System.exit(1)
      }
      val sparkConf = new SparkConf()
        .setAppName("LoadData_Alert")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer.max", "2000")
        // .setMaster("local")
      val sc = new SparkContext(sparkConf)
      val confRead = sc.textFile(args(0))
      val alertList = confRead.map(getConfInfo)
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
      /** connection redis server */
      val jedis = RedisUtil.getRedis(confInfoMap("ip"),confInfoMap("port"),confInfoMap("auth"),confInfoMap("database"))
      /** init data from redis such as stockCodes... */
      initRedisData()

      try{
        AlertHttp.init(confInfoMap)
        AlertHttp.alertSV(jedis, stockCodes)
        PrismLogger.info("alertSV followList number size--"+AlertHttp.followList.size)
        PrismLogger.info("alertSV searchList number size--"+AlertHttp.searchList.size)
        PrismLogger.info("alertSV visitList number size--"+AlertHttp.visitList.size)
      }catch {
        case e:Exception =>
          PrismLogger.info("alertSV Exception")
          PrismLogger.exception(e)
          SendMessage.sendMessage(1,"电信平台计算", "搜索和查看报警操作异常")
          System.exit(-1)
      }
      try{
        AlertHttp.alertF(jedis, stockCodes)
        PrismLogger.info("----------alertF followList number size--"+AlertHttp.followList.size)
        PrismLogger.info("----------alertF searchList number size--"+AlertHttp.searchList.size)
        PrismLogger.info("-----------alertF visitList number size--"+AlertHttp.visitList.size)
        PrismLogger.info("---Alert Over----")
        PrismLogger.info("Total Alert number--"+AlertHttp.sendNumber)
      }catch {
        case e:Exception =>
          PrismLogger.info("alertF Exception")
          PrismLogger.exception(e)
          SendMessage.sendMessage(1,"电信平台计算", "关注报警操作异常")
          System.exit(-1)
      }
      try{
        /** 添加 add follow 数据到 redis **/
        FollowStock.writeAddFollowDataToRedis(jedis,AlertHttp.followList)
        System.out.println("---add Follow Alert to Redis F---p.syncAndReturnAll()----")
      }catch {
        case e:Exception =>
          PrismLogger.info("add Follow Alert to Redis Exception")
          SendMessage.sendMessage(1,"电信平台计算", "写入关注数据操作异常")
          PrismLogger.exception(e)
          System.exit(-1)
      }finally{
        if(jedis != null){
          /** 关闭连接池的实例 **/
          jedis.close()
        }
      }
      sc.stop()
      System.exit(0)
    }
}
