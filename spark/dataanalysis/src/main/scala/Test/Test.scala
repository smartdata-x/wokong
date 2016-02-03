package Test

import _root_.util.FileUtil
import config.FileConfig
import hbase.TableHbase
import lengjing.LoadData
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/1/19.
  */
object Test {

  //  val RedisInfoList: mutable.MutableList[String] = mutable.MutableList[String]()
  //  var redisInfoMap = new mutable.HashMap[String,String]()
  //  var stockCodes:java.util.List[String] = new util.ArrayList[String]()
  //  var nameUrls = mutable.MutableList[String]()
  //  var jianPins = mutable.MutableList[String]()
  //  var quanPins = mutable.MutableList[String]()
  //
  //  def initRedisData(): Unit ={
  //    val jedis2 = RedisUtil.getRedis(redisInfoMap("ip"),redisInfoMap("port"),redisInfoMap("auth"),redisInfoMap("database"))
  //    System.out.println("redis connected")
  //    stockCodes = jedis2.lrange("stock:list", 0, -1)
  //    val iterator:util.Iterator[String] = stockCodes.iterator()
  //    while(iterator.hasNext){
  //      val stockCode = iterator.next()
  //      // System.out.println("stockCode:"+stockCode)
  //      nameUrls.+=(jedis2.get("stock:"+stockCode+":nameurl"))
  //      jianPins.+=(jedis2.get("stock:"+stockCode+":jianpin"))
  //      quanPins.+=(jedis2.get("stock:"+stockCode+":quanpin"))
  //    }
  //
  //  }
  //
  //  def main(args:Array[String]) {
  //    val RedisInfoList: mutable.MutableList[String] = mutable.MutableList[String]()
  //    // val urlString = "http://192.168.1.4/cgi-bin/hot_words_notice.fcgi"
  //    // val urlString = "http://192.168.1.4/cgi-bin/stock_notice.fcgi"
  //    val sparkConf = new SparkConf()
  //      .setAppName("LoadDataScala")
  //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //      .set("spark.kryoserializer.buffer.max", "2000").setMaster("local[*]")
  //    val sc = new SparkContext(sparkConf)
  //    val redisFileRead = sc.textFile(args(0))
  //    redisFileRead.foreach(println)
  //    val redisList = redisFileRead.map(LoadData.getConfInfo)
  //    // val file = sc.textFile(args(1))
  //    // info.foreach(print(_))  //(ip,222.73.34.96)(port,6390)(auth,7ifW4i@M)(database,0)
  //    redisList.collect().foreach {
  //      case (attr, value) =>
  //        if (attr == "ip") redisInfoMap.+=(("ip", value))
  //        if (attr == "port") redisInfoMap.+=(("port", value))
  //        if (attr == "auth") if (value != null) redisInfoMap.+=(("auth", value)) else redisInfoMap.+=(("auth", null))
  //        if (attr == "database") redisInfoMap.+=(("database", value))
  //        if (attr == "urlString") redisInfoMap.+=(("urlString", value))
  //        if (attr == "DELAYHOUR") redisInfoMap.+=(("DELAYHOUR", value))
  //        if (attr == "FOLLOW_NUMBER") redisInfoMap.+=(("FOLLOW_NUMBER", value))
  //        if (attr == "SEARCH_NUMBER") redisInfoMap.+=(("SEARCH_NUMBER", value))
  //        if (attr == "VISIT_NUMBER") redisInfoMap.+=(("VISIT_NUMBER", value))
  //    }
  //
  //    /** connection redis server */
  //    val jedis = RedisUtil.getRedis(redisInfoMap("ip"), redisInfoMap("port"), redisInfoMap("auth"), redisInfoMap("database"))
  //    //  val p = jedis.pipelined()
  //    /** init data from redis such as stockCodes... */
  //    initRedisData()
  //    try{
  //      AlertHttp.init(redisInfoMap)
  //      AlertHttp.alert(jedis, stockCodes)
  //      System.out.println("---热度报警 OVER----")
  //      println("热度报警数："+AlertHttp.SEND_NUMBER)
  //    }catch {
  //      case e:Exception =>
  //        println("Exception")
  //    }finally {
  //       /** 关闭连接词的实例 **/
  //       jedis.close()
  //       sc.stop()
  //    }
  //    // PrismLogger.info("---p.syncAndReturnA
  def main(args: Array[String]) {
//    val jsonString = "{\"down\" : 0, \"stock\" : \"\", \"url\" : \"http:\\/\\/field.10jqka.com.cn\\/20160122\\/c587480902.shtml\", \"up\" : 0, \"id\" : \"3cd8fb7d93df0b4f85ef4b971951640e_011920\", \"from\" : \"同花顺\", \"indus\" : \"\", \"sect\" : \"\", \"time\" : \"20160130011920\", \"title\" : \"煤企每卖一吨煤亏100多元 煤炭地板价尚在研究中\"}"
//    val resultString = JSON.parseFull (jsonString)
//    resultString.get.asInstanceOf[Map[String,String]].get("down")

    val sparkConf = new SparkConf()
        .setAppName("LoadDataScala")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer.max", "2000")
        .setMaster("local")
    val sc = new SparkContext(sparkConf)
    /** 获取 hbase的follow 信息 **/
    // val list = FileUtil.readFile(FileConfig.ROOT_DIR + FileConfig.HBASE_CONF_FILENAME)
    val list = FileUtil.readFile(FileConfig.TEST_ROOT + FileConfig.TEST_HBASE_CONF_FILENAME)
    var timeRangeMap = new mutable.HashMap[String,String]()
    list.foreach(x =>{
      val split = x.split("=")
      timeRangeMap.+=(split(0) -> split(1))
    })
    println("timeRange>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>:"+timeRangeMap("last_timeStamp")+":")
    TableHbase.getStockCodesFromHbase(sc,timeRangeMap("last_timeStamp"))
    System.out.println("---Follow stockCodeMap size:----"+LoadData.stockCodeMap.size)
    System.out.println("---Follow userMap size:----"+LoadData.userMap.size)
  }

}