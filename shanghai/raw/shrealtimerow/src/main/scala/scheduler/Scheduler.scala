package scheduler

import config.{FileConfig, XMLConfig}
import kafka.serializer.StringDecoder
import log.SUELogger
import message.TextSender
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.{FileUtil, HDFSUtil, StringUtil, TimeUtil}

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

    if (args.length < 11) {

      System.err.println(
        """
          |Usage: LoadData <brokers> <topics> <zkhosts> <dataDir> <searchEngineDataDir> <errorDataDir>
          |<logConfDir> <logDir> <dataerrorlogdir> <messageXmlFile> <checkpoint_dir>
          |<brokers> is a list of one or more Kafka brokers
          |<topics> is a list of one or more kafka topics to consume from
          |<zkhosts> is a list of zookeeper to consume from
          |<dataDir> is the data saving path
          |<searchEngineDataDir> is the search engine data saving path
          |<errorDataDir>  exception data saving path
          |<logConfDir> log conf path
          |<logDir> data mumber count dir
          |<dataerrorlogdir> saving data length is not 8
          |<messageXmlFile> message notification conf file
          |<checkpoint_dir> data check point dir for exception
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

  def reformatData(str: String) = str.replaceAll("\n","").replaceAll("\r","")

  def main(args: Array[String]) {

    showWarning(args)

    val sparkConf = new SparkConf()
      .setAppName("Data_Analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.cleaner.ttl", "10000")
      .set("spark.akka.frameSize","256")

    val Array(brokers, topics, zks, dataDir, searchEngineDataDir, errorDataDir, logConfigDir, logDir, dataLog, xmlFile, checkpoint_dir) = args

    /* HDFSConfig.nameNode(nameNode)
    HDFSConfig.rootDir(dataDir)
    HDFSConfig.searchEngineDir(searchEngineDataDir)
    HDFSConfig.errorDataDir(errorDataDir) */

    FileConfig.rootDir(dataDir)
    FileConfig.searchEngineDir(searchEngineDataDir)
    FileConfig.errorDataDir(errorDataDir)
    FileConfig.logDir(logDir)
    FileConfig.dataLog(dataLog)
    SUELogger.logConfigureFile(logConfigDir)
    val messageConfig = XMLConfig.apply(xmlFile)

    // checkout point
    def createStreamingContext: () => StreamingContext = {
      val ssc = new StreamingContext(sparkConf, Seconds(60))
      ssc.checkpoint(checkpoint_dir)
      () => ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpoint_dir, creatingFunc = createStreamingContext)

    val text = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (
      ssc,
      kafkaParams = Map[String,String]("metadata.broker.list" -> brokers,"group.id" -> "Telecom","zookeeper.connect" -> zks,"serializer.class" -> "kafka.serializer.StringEncoder"),
      topics = topics.split(",").toSet
    )

    SUELogger.warn("write data")
    val result = text.flatMap(x =>flatMapFun(x._2))

    /** write data to local file */
    try {

      result.foreachRDD(org => {

        val rdd = org.filter(x => x != "" || !x.isEmpty).map(reformatData)

        val minCount = rdd.count()
        val time = TimeUtil.getMinute

        // data number per minute count
        val logData = Array(time + "\t" + minCount)
        FileUtil.saveLog(FileConfig.LOG_DIR, logData)
        // length error data count log
        val dataLog = Array(time + "\t" + rdd.filter(_.split("\t").length != 8).count())
        FileUtil.saveLog(FileConfig.DATA_LENGTH_LOG, dataLog)

        // search data
        val searchEngineData = rdd.filter(isSearchEngineURL).collect()
        FileUtil.saveData(FileConfig.SEARCH_ENGINE_DATA, searchEngineData)
        // data
        val resArray = rdd.collect()
        FileUtil.saveData(FileConfig.ROOT_DIR, resArray)


      })
    } catch {
      case e:Exception =>
        SUELogger.exception(e)
        val res = TextSender.send(messageConfig.KEY, messageConfig.MESSAGE_CONTEXT , messageConfig.RECEIVER)
        if(res) SUELogger.error("[SUE] MESSAGE SEND SUCCESSFULLY")
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
