package com.kunyan.scheduler

import java.io.{File, StringReader}
import java.text.SimpleDateFormat
import java.util.Date
import javax.xml.parsers.DocumentBuilderFactory

import _root_.redis.clients.jedis.Jedis
import akka.io.Udp.SO.Broadcast
import com.kunyan.config.RedisConfig
import com.kunyan.log.HWLogger
import com.kunyan.net.HotWordHttp
import com.kunyan.util.TimeUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.w3c.dom.Element
import org.wltea.analyzer.IKSegmentation

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source



/**
  * Created by yangshuai on 2016/2/25.
  */
object Scheduler {

  //至少保证此list在初始化时有一个元素
  val tablePrefix = List[Int](3, 5, 6, 7, 8, 9)

  var timer = 0
  var total = 0

  val mapAfter = new mutable.HashMap[String, Int]()
  val mapBefore = new mutable.HashMap[String, Int]()
  val mapDiff = new mutable.HashMap[String, Int]()
  val hbaseConf = HBaseConfiguration.create()
  val conf = new SparkConf().setAppName("hot words")
  val sc = new SparkContext(conf)

  def getRddByTableName(tableName: String): RDD[(ImmutableBytesWritable, Result)] = {
    init(tableName)
    sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable], classOf[Result])
  }

  def getWordRank(pair: (String, Iterable[String])): (String, Seq[(String, Int)]) = {

    val key = pair._1
    val iterator = pair._2

    var list = ListBuffer[String]()

    iterator.foreach(x => {
      list ++= splitWords(x)
    })

    val map = mutable.HashMap[String, Int]()
    for (elem <- list) {
      if (map.get(elem).isEmpty) {
        map.put(elem, 1)
      } else {
        val count = map.get(elem).get
        map.put(elem, count + 1)
      }
    }

    val seq = map.toSeq.sortWith(_._2 > _._2)

    val rankMap = mutable.HashMap[String, Int]()

    var rank = 0
    var i = 0
    var preCount = Int.MaxValue

    seq.foreach(x => {
      i += 1
      val count = x._2
      if (count < preCount) {
        rank = i
        preCount = count
      }
      rankMap.put(x._1, rank)
    })

    (key, rankMap.toSeq)
  }

  def convertFormat(pair: (String, Seq[(String, Int)])): (String, String) = {

    var value = ""

    pair._2.foreach(x => {
      value += x._1 + "->" + x._2 + ","
    })

    (pair._1, value)
  }

  /**
    * 获取前一个小时的热词数据
    *
    * @return
    */
  def getLastHourHotWords: mutable.HashMap[String, ListBuffer[(String, Int)]] = {

    val jedis = new Jedis("222.73.34.96", 6390)
    jedis.auth("7ifW4i@M")

    val map = mutable.HashMap[String, ListBuffer[(String, Int)]]()
    val list = ListBuffer[(String, Int)]()

    jedis.hgetAll("hotwordsrank:" + TimeUtil.getPreHourStr).asScala.map(x => {
      x._2.split(",").foreach(y => {
        val arr = y.split("->")
        list += arr(0) -> arr(1).toInt
      })
      map.put(x._1, list)
    })

    map
  }

  /**
    * 计算新增自选股所占比率并存到redis
    */
  def sendHotWords(wordList: Seq[(String, String)]): Unit = {

    val jedis = new Jedis("222.73.34.96", 6390)
    jedis.auth("7ifW4i@M")
    val pipeline = jedis.pipelined()

    wordList.map(x => {
      if (x._1.length > x._1.indexOf('_') + 1) {
        pipeline.hset("hotwordsrank:" + TimeUtil.getDay + "-" + TimeUtil.getCurrentHour, x._1, x._2)
        pipeline.expire("hotwordsrank:" + TimeUtil.getDay + "-" + TimeUtil.getCurrentHour, 60 * 60 * 2)
      }
    })

    pipeline.sync()
    jedis.quit
  }

  def initRedis(path: String): Unit = {

    val file = new File(path)

    val document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(file)
    val redisRoot = document.getElementsByTagName("redis").item(0).asInstanceOf[Element]
    val ip = redisRoot.getElementsByTagName("ip").item(0).getTextContent
    val port = redisRoot.getElementsByTagName("port").item(0).getTextContent
    val auth = redisRoot.getElementsByTagName("auth").item(0).getTextContent

    RedisConfig.init(ip, port.toInt, auth)
  }

  //得到hbase里的数据并作为RDD
  def initRdd(): Unit = {

    //为conf设置时间范围
    setTimeRange()

    var rdd = getRddByTableName(tablePrefix.head + "_analyzed")

    tablePrefix.slice(1, tablePrefix.size).foreach(x => {
      val tableName = x + "_analyzed"
      val tempRdd = getRddByTableName(tableName)
      rdd = rdd.union(tempRdd)
    })

    //排序+topn
    val topWords = rdd.flatMap(convertRawToMap)
      .groupByKey()
      .map(getWordRank).persist(StorageLevel.MEMORY_AND_DISK)

    val result = topWords.map(convertFormat).collect()

    sendHotWords(result.toSeq)

    val oldMap = sc.broadcast(getLastHourHotWords)

    HWLogger.warn("before loop")

    val pairs = topWords.map(x => {

      HWLogger.warn("enter rdd loop")

      val newWords = x._2

      val result = mutable.HashMap[String, Int]()

      var oldWords:scala.collection.immutable.Map[String, Int] = null

      if (oldMap.value.get(x._1).nonEmpty) {

        oldWords = oldMap.value.get(x._1).get.toMap[String, Int]
        val oldSize = oldWords.size + 1
        newWords.foreach(newWord => {
          val hotWord = newWord._1
          val newRank = newWord._2

          val oldRank = oldWords.getOrElse(hotWord, oldSize)
          val rank = oldRank - newRank

          result.put(hotWord, rank)
        })

      } else {
        newWords.foreach(newWord => {
          result.put(newWord._1, 0 - newWord._2)
        })
      }

      val list = result.toSeq.sortWith(_._2 > _._2).toList

      var size = result.size
      if (size > 5)
        size = 5

      if (size > 0) {

        var hotWords = ""

        for (i <- 0 until size) {
          hotWords += list(i)._1 + "*"
        }


        (x._1, hotWords)
      }

    }).collect()


    total = pairs.length

    pairs.foreach(x => {

      val pair = x.asInstanceOf[Tuple2[String, String]]

      val arr = pair._1.split("_")
      var keyValue = ""
      if (arr.length > 1)
        keyValue = arr(1)


      val ttype = pair._1.split("_")(0)
      var key = ""
      if (ttype == "se") {
        key = "section"
      } else if (ttype == "in") {
        key = "industry"
      } else {
        key = "stock_code"
        keyValue += "x"
      }

      val paramMap = new mutable.HashMap[String, String]
      paramMap.clear()
      paramMap.+=("hot_words" -> pair._2, key -> keyValue)
      HotWordHttp.sendNew("http://222.73.34.104/cgi-bin/northsea/prsim/subscribe/1/hot_words_notice.fcgi", paramMap)
    })

  }

  /**
    * 分词的方法
    *
    **/
  //TODO 换成新算法
  def splitWords(title: String): ListBuffer[String] = {

    var list = ListBuffer[String]()

    val reader: StringReader = new StringReader(title)
    val ik = new IKSegmentation(reader, true)
    var lexeme = ik.next()

    while (lexeme != null) {

      val word = lexeme.getLexemeText
      list += word.toString
      lexeme = ik.next()
    }

    list
  }

  /**
    * 将hbase里面的数据转换为map
    */
  def convertRawToMap(tuple: (ImmutableBytesWritable, Result)): mutable.HashMap[String, String] = {

    val result = tuple._2
    val title = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("title")))
    val section = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("section"))).split(",")
    val stock = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("category"))).split(",")
    val industry = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("industry"))).split(",")

    val map = mutable.HashMap[String, String]()

    section.foreach(x => {
      map.put("se_" + x, title)
    })

    stock.foreach(x => {
      map.put("st_" + x, title)
    })

    industry.foreach(x => {
      map.put("in_" + x, title)
    })

    map
  }

  //获取时间戳
  def setTimeRange(): Unit = {

    val scan = new Scan()
    val date = new Date(new Date().getTime - 60 * 60 * 1000)
    val format = new SimpleDateFormat("yyyy-MM-dd HH")
    val time = format.format(date)
    val time1 = format.format(new Date().getTime)
    val startTime = time + "-00-00"
    val stopTime = time1 + "-00-00"
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
    val startRow: Long = sdf.parse(startTime).getTime
    val stopRow: Long = sdf.parse(stopTime).getTime


    scan.setTimeRange(startRow, stopRow)
    val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)
    hbaseConf.set(TableInputFormat.SCAN, scanToString)
  }

  def init(tb: String): Unit = {

    hbaseConf.set("hbase.rootdir", "hdfs://ns1/hbase")
    hbaseConf.set("hbase.zookeeper.quorum", "server0,server1,server2")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tb)
    hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "info")
    hbaseConf.set(TableInputFormat.SCAN_COLUMNS, "title")

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  }

  def main(args: Array[String]) {
    try {
      initRdd()
      HWLogger.warn("finish init")
    } catch {
      case e: Exception =>
        HWLogger.exception(e)
    } finally {
      sc.stop
    }
  }

}
