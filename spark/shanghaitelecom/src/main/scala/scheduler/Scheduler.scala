package scheduler

import config.HDFSConfig
import kafka.serializer.StringDecoder
import log.SUELogger
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.{HDFSUtil, StringUtil, TimeUtil}

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/2/23.
  * 电信数据解析主程序
  */
object Scheduler {

  /**
    * 解压缩电信数据
    * @param line 源数据字符串
    * @return 解压缩后的字符串数组
    */
  def flatMapFun(line: String): mutable.MutableList[String] = {

    val lineList: mutable.MutableList[String] = mutable.MutableList[String]()
    // val res = StringUtil.parseJsonObject(line)
    val res = StringUtil.parseNotZlibJsonObject(line)

    if(res.nonEmpty){
      lineList.+=(res)
    }

    lineList

  }

  /**
    * 过滤出含有关键字的数据
    * @param str 源数据字符串
    * @return 关键字是否存在，true：存在，反之不存在
    */
  def isSearchEngineURL(str:String):Boolean = {

    val keyWord = str.split("\t")(7)

    if(keyWord !="NoDef") {
      true
    } else {
      false
    }
  }

  /**
    * 传入参数出错异常提示
    * @param args 参数数组
    */
  def showWarning(args: Array[String]): Unit = {

    if (args.length < 6) {

      System.err.println(
        """
          |Usage: LoadData <brokers> <topics> <zkhosts> <dataDir> <searchEngineDataDir>
          |<brokers> is a list of one or more Kafka brokers
          |<topics> is a list of one or more kafka topics to consume from
          |<zkhosts> is a list of zookeeper to consume from
          |<nameNode> the cluster ip
          |<dataDir> is the data saving path
          |<searchEngineDataDir> is the search engine data saving path
        """.stripMargin)

      System.exit(1)
    }
  }

  /**
    * 写入hadoop fs
    * @param dir  指定目录
    * @param data 数据
    */
  def saveData(dir: String, data:Array[String]): Unit = {

    HDFSUtil.mkDir(new Path(dir))
    val day = new Path(dir + "/" + TimeUtil.getDay)
    HDFSUtil.mkDir(day)
    val file = HDFSUtil.createFile(dir + "/" + TimeUtil.getDay)
    HDFSUtil.saveData(file,data)

  }

  def main(args: Array[String]) {

    showWarning(args)

    val sparkConf = new SparkConf()
      .setAppName("Data_Analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.cleaner.ttl", "10000")

    val Array(brokers, topics, zkhosts, dataDir,searchEngineDataDir,errorDataDir, nameNode) = args

    HDFSConfig.nameNode(nameNode)
    HDFSConfig.rootDir(dataDir)
    HDFSConfig.searchEngineDir(searchEngineDataDir)
    HDFSConfig.errorDataDir(errorDataDir)

    val ssc = new StreamingContext(sparkConf,Seconds(2))

    val text = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (
      ssc,
      kafkaParams = Map[String,String]("metadata.broker.list" -> brokers,"group.id" -> "Telecom","zookeeper.connect" -> zkhosts,"serializer.class" -> "kafka.serializer.StringEncoder"),
      topics = topics.split(",").toSet
    )

    SUELogger.warn("write data")
    val result = text.flatMap(x =>flatMapFun(x._2))

    try {

      result.foreachRDD(rdd => {

        // search data
        val searchEngineData = rdd.filter(isSearchEngineURL)
          .collect()
        HDFSUtil.saveToHadoop(HDFSConfig.HDFS_SEARCH_ENGINE_DATA,searchEngineData)
        // data
        val resArray = rdd.collect()
        HDFSUtil.saveToHadoop(HDFSConfig.HDFS_ROOT_DIR, resArray)

      })
    } catch {
      case e:Exception =>
        SUELogger.exception(e)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
