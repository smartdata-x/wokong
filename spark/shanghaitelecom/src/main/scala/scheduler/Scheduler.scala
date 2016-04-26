package scheduler

import util.{HBaseUtil, TimeUtil, FileUtil, StringUtil}
import classification._
import config.FileConfig
import kafka.serializer.StringDecoder
import log.SUELogger

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Created by C.J.YOU on 2016/2/23.
  * 电信数据解析主程序
  */
object Scheduler {

  def flatMapFun(line: String): mutable.MutableList[String] = {
    val lineList: mutable.MutableList[String] = mutable.MutableList[String]()
    val res = StringUtil.parseJsonObject(line)
    // val res = line
    if(res.nonEmpty){
      lineList.+=(res)
    }
    lineList
  }

  def isSearchEngineURL(str:String):Boolean = {
    val keyWord = str.split("\t")(7)
    if(keyWord !="NoDef") true else false
    // if(url.contains("so.com")|| url.contains("bing.com") || url.contains("baidu.com") || url.contains("sogou.com")) true else false
  }

  def showWarning(args: Array[String]): Unit ={
    if (args.length < 5) {
      System.err.println(
        """
          |Usage: LoadData <brokers> <topics> <zkhosts> <dataDir> <searchEngineDataDir>
          |<brokers> is a list of one or more Kafka brokers
          |<topics> is a list of one or more kafka topics to consume from
          |<zkhosts> is a list of zookeeper to consume from
          |<dataDir> is the data saving path
          |<searchEngineDataDir> is the search engine data saving path

        """.stripMargin)
      System.exit(1)
    }
  }

  def main(args: Array[String]) {

    showWarning(args)

    val sparkConf = new SparkConf()
      .setAppName("Data_Analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.cleaner.ttl", "10000")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)
    val Array(brokers, topics, zkhosts,dataDir,searchEngineDataDir,errorDataDir) = args
     // val Array(brokers, topics, zkhosts) = args
    // 配置数据保存的路径
    FileConfig.rootDir(dataDir)
    FileConfig.searchEngineDir(searchEngineDataDir)
    FileConfig.errorDataDir(errorDataDir)

    val ssc = new StreamingContext(sparkConf,Seconds(60))
    val numStream = 5

    val lineData = (1 to numStream).map{ i => KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (
      ssc,
      kafkaParams = Map[String,String]("metadata.broker.list" -> brokers,"group.id" -> "Telecom","zookeeper.connect" -> zkhosts,"serializer.class" -> "kafka.serializer.StringEncoder"),
      topics = topics.split(",").toSet
    ) }

    val text = ssc.union(lineData)
    SUELogger.warn("write data")
    val result = text.flatMap(x =>flatMapFun(x._2))

    /** write data to local file */
    try {
      result.foreachRDD(rdd => {
        val searchEngineData = rdd.filter(isSearchEngineURL).collect()
        FileUtil.saveData(FileConfig.SEARCH_ENGINE_DATA,searchEngineData)
        // HBaseUtil.saveData(searchEngineData)
        val resArray = rdd.collect()
        FileUtil.saveData(FileConfig.ROOT_DIR,resArray)
        // HBaseUtil.saveData(resArray)
      })
    } catch {
      case e:Exception =>
        SUELogger.error("save data error!!!!!!!!!!!!!!!!!!!!")
        System.exit(-1)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
