package scheduler

import java.util


import _root_.util.{TimeUtil, FileUtil, RedisUtil}

import config.FileConfig

import hbase.TableHbase
import log.PrismLogger
import message.SendMessage

import org.apache.spark.{SparkContext, SparkConf}


import scala.collection.mutable


/**
  * Created by C.J.YOU on 2016/1/19.
  * 获取hbase follow 数据的 主程序
  */
object Scheduler {

  var confInfoMap = new mutable.HashMap[String,String]()
  var stockCodes:java.util.List[String] = new util.ArrayList[String]()
  var words  = new mutable.HashMap[String,Int]()

  def getConfInfo(line :String):(String,String) = {
    val  lineSplits: Array[String] = line.split("=")
    val  attr = lineSplits(0)
    val confValue = lineSplits(1)
    (attr,confValue)
  }

  def flatMapFun(timeStam:String,stockCodesTemp:Set[String]): mutable.MutableList[String] = {
    val lineList: mutable.MutableList[String] = mutable.MutableList [String]()
    val timeStamp = timeStam
    val hour = TimeUtil.getTime(timeStamp)
    /** visit */
    if (stockCodesTemp.nonEmpty  &&  timeStamp != null) {
      for (stockCode <- stockCodesTemp) {
        if(stockCodes.contains(stockCode)) {
          lineList += ("hash:follow:" + hour + "," + stockCode)
          lineList += ("follow:count:" + hour)
          lineList += ("follow:" + stockCode + ":" + hour)
        }
      }
    }
    lineList
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: LoadData <redis Conf file> ")
      System.exit(-1)
    }
    val sparkConf = new SparkConf()
      .setAppName("LoadData_Follow")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)
    val confRead = sc.textFile(args(0))
    val alertList = confRead.map(getConfInfo)
    //(ip,222.73.34.96)(port,6390)(auth,7ifW4i@M)(database,0)
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
    stockCodes = jedis.lrange("stock:list", 0, -1)
    /** 获取 hbase的follow 信息 **/
    val list = FileUtil.readFile(FileConfig.ROOT_DIR + FileConfig.HBASE_CONF_FILENAME)
     // val list = FileUtil.readFile(FileConfig.TEST_ROOT + FileConfig.TEST_HBASE_CONF_FILENAME)
    var timeRangeMap = new mutable.HashMap[String,String]()
    list.foreach(x =>{
      val split = x.split("=")
      timeRangeMap.+=(split(0) -> split(1))
    })
    println("timeRange>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>:"+timeRangeMap("last_timeStamp")+":"+TimeUtil.getTime(timeRangeMap("last_timeStamp")))
    PrismLogger.info("timeRange>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>:"+timeRangeMap("last_timeStamp")+":"+TimeUtil.getTime(timeRangeMap("last_timeStamp")))
    try{
      TableHbase.getStockCodesFromHbase(sc,timeRangeMap("last_timeStamp"))
      System.out.println("---Follow stockCodeMap size:----"+TableHbase.stockCodeMap.size)
      PrismLogger.info("---Follow stockCodeMap size:----"+TableHbase.stockCodeMap.size)
      if(TableHbase.stockCodeMap.isEmpty){
        SendMessage.sendMessage(2,"电信平台计算", " Hbase数据异常")
        System.exit(-1)
      }
      System.out.println("---Follow userMap size:----"+TableHbase.userMap.size)
      val result  = sc.parallelize(TableHbase.stockCodeMap.toSeq).flatMap(x =>flatMapFun(x._1,x._2)).map((_,1)).reduceByKey(_+_).collect()
      println("result size:"+result.length)
      result.foreach(line => {
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
      p.syncAndReturnAll()
      System.out.println("---F---p.syncAndReturnAll()----")
    } catch {
    case e:Exception =>
      PrismLogger.info(" F Operation Exception")
      jedis.close()
      SendMessage.sendMessage(1,"电信平台计算", "过去关注数据操作异常")
      PrismLogger.exception(e)
      System.exit(-1)
    }finally{
      if(jedis != null){
        /** 关闭连接的实例 **/
        jedis.close()
      }
    }
    PrismLogger.info("--- Write timeStamp for hbase----<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< ----")
    System.out.println("--- Write timeStamp for hbase----<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< ----")
    FileUtil.createFile(FileConfig.ROOT_DIR + FileConfig.HBASE_CONF_FILENAME,"last_timeStamp="+System.currentTimeMillis())
    sc.stop()
    System.exit(0)
  }

}