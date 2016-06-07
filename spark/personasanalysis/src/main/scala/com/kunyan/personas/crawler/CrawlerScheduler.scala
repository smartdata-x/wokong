package com.kunyan.personas.crawler

import com.kunyan.personas.config.SparkConfig
import com.kunyan.personas.crawler.request.Request
import com.kunyan.personas.extractor.Extractor
import com.kunyan.personas.util.StringUtil
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/5/12.
  */
object CrawlerScheduler {

  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("USER")
    .set("spark.serializer",SparkConfig.SPARK_SERIALIZER)
    .set("spark.kryoserializer.buffer.max",SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  val ssc = new StreamingContext(sc,Seconds(10))

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

    val Array(brokers, topics, zkhosts) = args

    val lineData = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (
      ssc,
      kafkaParams = Map[String,String]("metadata.broker.list" -> brokers,"group.id" -> "CrawlerPersonas","zookeeper.connect" -> zkhosts,"serializer.class" -> "kafka.serializer.StringEncoder"),
      topics.split(",").toSet
    )
    val result = lineData.flatMap(x =>flatMapFun(x._2))
    result.foreachRDD { rdd =>
      val data = rdd.distinct()
      println("start extract user info:")
      val userInfo  = data.map(_.split("\t"))
        .filter(_.length == 8)
        .filter(x => x(6) != "NoDef")
        .filter(x => x(3).contains("weibo.com"))
        .map(Extractor.getWeibo)
        .filter(_._1 !="")
        .distinct()
        .map(x => Request.sendRequest(x._2,x._1,x._3))
        .filter(_!="")
        .foreach(println)
    }

    ssc.start()
    ssc.awaitTermination()

    //Request.requestForUserInfo("1005055788957514","","")
  }
}
