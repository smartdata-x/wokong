package test

import java.text.SimpleDateFormat
import java.util.Date

import config.FileConfig
import kafka.serializer.StringDecoder
import log.SUELogger
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import util.{FileUtil, HBaseUtil, StringUtil, TimeUtil}

import scala.collection.mutable


/**
  * Created by C.J.YOU on 2016/2/23.
  * 电信数据解析主程序
  */
object TestScheduler {

  def flatMapFun(line: String): mutable.MutableList[String] = {
    val lineList: mutable.MutableList[String] = mutable.MutableList[String]()
    val res = TestStringUtil.parseJsonObject(line)
    // val res = line
    if(res.nonEmpty){
      lineList.+=(res)
    }
    lineList
  }

  val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")

  def getTime: String = {
    sdf.format(new Date())
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
    val Array(brokers, topics, zkhosts) = args
    val ssc = new StreamingContext(sparkConf,Seconds(60))
    val numStream = 5
    val sc = ssc.sparkContext

    val lineData = (1 to numStream).map{ i => KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (
      ssc,
      kafkaParams = Map[String,String]("metadata.broker.list" -> brokers,"group.id" -> "Telecom","zookeeper.connect" -> zkhosts,"serializer.class" -> "kafka.serializer.StringEncoder"),
      topics = topics.split(",").toSet
    ) }
   /* val text = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (
      ssc,
      kafkaParams = Map[String,String]("metadata.broker.list" -> brokers,"group.id" -> "Telecom","zookeeper.connect" -> zkhosts,"serializer.class" -> "kafka.serializer.StringEncoder"),
      topics = topics.split(",").toSet
    )*/
    val text = ssc.union(lineData)
    SUELogger.warn("write data")
    val result = text.flatMap(x =>flatMapFun(x._2))

    /** write data to local file */
    try {
      val accValue = sc.accumulator(0)
      val ts  = getTime
      var index = accValue.value
      result.foreachRDD(rdd => {
        rdd.foreach(record =>{
          // FileUtil.writeStringToFile("F:\\datatest\\data\\"+ts,ts + "_ky_" +index+"--->" + record)
          index = index + 1
        })
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
