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

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        """
          |Usage: LoadData <RedisConf> <brokers> <topics> <zkhosts>
          |<brokers> is a list of one or more Kafka brokers
          |<topics> is a list of one or more kafka topics to consume from
          |<zkhosts> is a list of zookeeper to consume from

        """.stripMargin)
      System.exit(1)
    }
    val sparkConf = new SparkConf()
      .setAppName("Data_Analysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.cleaner.ttl", "10000")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)
    val Array(brokers, topics, zkhosts) = args
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val text = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (
      ssc,
      kafkaParams = Map[String,String]("metadata.broker.list" -> brokers,"group.id" -> "Telecom","zookeeper.connect" -> zkhosts,"serializer.class" -> "kafka.serializer.StringEncoder"),
      topics = topics.split(",").toSet
    )
    SUELogger.warn("write data")
    val result = text.flatMap(x =>flatMapFun(x._2))

    /** write data to local file */
    try {
      result.foreachRDD(rdd => {
        val resArray = rdd.collect()
        val dir = FileConfig.ROOT_DIR + "/" + TimeUtil.getDay
        val file = FileConfig.ROOT_DIR + "/" + TimeUtil.getDay +"/"+TimeUtil.getCurrentHour
        FileUtil.mkDir(dir)
        FileUtil.createFile(file)
        FileUtil.writeToFile(file,resArray)
        HBaseUtil.saveData(resArray)
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
