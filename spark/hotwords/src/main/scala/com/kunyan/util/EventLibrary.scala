package com.wangcao.learning

import java.text.SimpleDateFormat
import java.util.Date

import com.ibm.icu.text.CharsetDetector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import com.kunyan.nlpsuit.util.TextPreprocessing.process
import scala.collection.mutable.ArrayBuffer

/**
  * Created by cc on 2016/4/22.
  */
  
object EventLibrary {

  val tablePrefix = List[Int](3, 5, 6, 7, 8)
  val hbaseConf = HBaseConfiguration.create()
  val sparkConf = new SparkConf().setAppName("eventLibrary")//.setMaster("local")
  val sc = new SparkContext(sparkConf)


  def getHbaseConf(): Configuration = {

    hbaseConf.set("hbase.rootdir", "hdfs://master/hbase")
    hbaseConf.set("hbase.zookeeper.quorum", "master,slave1,slave2,slave3,slave4")
    hbaseConf

  }

  def judgeCharser(html: Array[Byte]): String = {

    val icu4j = new CharsetDetector()
    icu4j.setText(html)
    val encoding = icu4j.detect()
    encoding.getName

  }

  def setTimeRange(): Unit = {

    val scan = new Scan()
    val date = new Date(new Date().getTime - 60 * 60 * 1000 * 48)
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

  def getHbaseRdd(tableName: String): RDD[(ImmutableBytesWritable, Result)] = {

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val hbaseConf = getHbaseConf()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    setTimeRange()
    val hbaseRdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable], classOf[Result])
    hbaseRdd

  }

  def getTableA(): RDD[String] = {

    val news = getHbaseRdd("wk_detail").map(x => {
      val a = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("url"))
      val b = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("title"))
      val c = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))

      val formata = judgeCharser(a)
      val formatb = judgeCharser(b)
      val formatc = judgeCharser(c)
      new String(a, formata) + "\t" + new String(b, formatb) + "\t" + new String(c, formatc).replaceAll("\\&[a-zA-Z]{1,10};", "")
        .replaceAll("<[^>]*>", "")
        .replaceAll("\n", "")
        .replaceAll("\t", "")
    })

    val processedNews = news.map(_.split("\t"))
      .filter(x => x.size == 3)
      .filter(x => !x(2).contains("/*正文内嵌内容*/"))
      .filter(x => !x(2).contains("container"))
      .filter(x => !x(2).contains("title : {"))
      .filter(x => !x(2).contains("var "))
      .filter(x=> !x(2).contains("宋体"))
      .map(x => x(0) + "\t" + x(1) + "\t" + x(2))

    processedNews

  }

  def getTableB(): RDD[(String,String)] = {

    var rddUnion = getHbaseRdd(tablePrefix.head + "_analyzed")

    tablePrefix.slice(1, tablePrefix.size).foreach(x => {
      val tableName = x + "_analyzed"
      val tempRdd = getHbaseRdd(tableName)
      rddUnion = rddUnion.union(tempRdd)
    })
    val news = rddUnion.map(x => {
      val a = x._2.getRow()
      val b = x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("category"))
      val c = x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("industry"))
      val d = x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("section"))

      val formata = judgeCharser(a)
      val formatb = judgeCharser(b)
      val formatc = judgeCharser(c)
      val formatd = judgeCharser(d)
      new String(a, formata) + "\t" + new String(b, formatb) + "\t" + new String(c, formatc) + "\t" + new String(d, formatd)
    })

    val processedNews = news.map(_.split("\t")).filter(x => x.length == 4)
      .filter(x => x(1) != "" && x(2) != "" && x(3) != "" )
      .map(x=>(x(0),x(1)+" "+x(2)+" "+x(3)))

    processedNews

  }

  def processWord (hotWords:RDD[(String,String)],pre:String): RDD[(String,Set[String])] = {

    val arr = new ArrayBuffer[String]()
    val hotWordsList = hotWords.collect
    for (line <- hotWordsList) {
      val list = line._1.split(",").toList
      for (i <- list) {
        arr += pre + i + "\t" + line._2
      }
    }

    val arrRdd = sc.parallelize(arr).map(_.split("\t")).map(x => (x(0),x(1).split(" ").toSet))

    arrRdd

  }

  def main(args: Array[String]): Unit = {

    /**
      * 1.分别读取hbase的两类表
      * tableA:  url title content
      * tableB:  url category  industry  section
      */

    val table1 = getTableA
    val table2 = getTableB
    table1.cache()
    table2.cache()

    /**
      * 2.筛选出标题中长度为2-8的引号中的词，这些词默认为关键词，
      */
    val title = table1.map(_.split("\t")).map(x => (x(0),x(1))).filter(x => x._2.contains("“") && x._2.contains("”"))
    title.cache()
    val specialWord1 = title
      .map(x => {
        val title = x._2
        val word =
          try {
            title.substring(title.indexOf("“")+1, title.indexOf("”"))
          } catch {
            case e: Exception =>
              null
          }
        (x._1,word)
      }).filter(x => x._1 !=  null && x._2 != null)

    val specialWord2 = title
      .map(x => {
        var word = "1"
        val title = x._2
        val backTitle =
          try {
            title.substring(title.indexOf("”")+1,title.length())
          } catch {
            case e: Exception =>
              null
          }
        if (backTitle.contains("“") && backTitle.contains("”")) {
         word = backTitle.substring(backTitle.indexOf("“")+1,backTitle.indexOf("”"))
        } else {
          word = null
        }
        (x._1,word)
      }).filter(x => x._1 !=  null && x._2 != null)

    val specialWord = specialWord1.union(specialWord2).filter(x => x._2.length >= 2 && x._2.length <= 8).join(table2).map(x => (x._2._2,x._2._1))

    /**
      * 3. 标题与正文分词
      */
    //3.1 获取停用词
    val stopWords = sc.textFile("/user/wangcao/stop_words_CN").collect
    val stopWordsBr = sc.broadcast(stopWords)

    //3.2调用分词程序
    val segWord = table1.map(_.split("\t"))
      .map(x => (x(0),x(1) + "111111" + x(2)))
      .map(x => (x._1,process(x._2,stopWordsBr).mkString(",")))
    segWord.cache()

    /**
      * 4.计算IDF值，创建语料库
      */
    //4.1 计算词项频率TF值,取标题与正文
    val totalWords = segWord.map(x=>x._2).map(_.replace("111111", "")).map(_.split(",")).map(x => x.toSeq)
    val docTermFreqs = totalWords.map(terms => {
      val termFreqs = terms.foldLeft(new scala.collection.mutable.HashMap[String, Int]()) {
        (map, term) => {
          map += term -> (map.getOrElse(term, 0) + 1)
          map
        }
      }
      termFreqs
    })
    docTermFreqs.cache()

    //4.2 计算逆文档频率idf值
    val docFreqs = docTermFreqs.flatMap(_.keySet).map((_, 1)).reduceByKey(_ + _)
    val numDocs = totalWords.count()
    val idfs = docFreqs.map {
      case (term, count) => (term, math.log(numDocs.toDouble / count))
    }.collect.toMap

    /**
      * 5. 筛选出有金融价值的文章，并获取这些文章的标题
      */
    //5.1 建立三个实体词典：股票，行业，概念
    val industryFile = sc.textFile("/user/wangcao/industry_words.words")
    val sectionFile = sc.textFile("/user/wangcao/section_words.words")
    val stockFile = sc.textFile("/user/wangcao/stock_words.words")

    val industryWords = industryFile.map(_.split("\t")).map(x => x(0)).distinct()
    val sectionWords = sectionFile.map(_.split("\t")).map(x => x(0)).distinct()
    val stockWords1 = stockFile.map(_.split("\t")).flatMap(x => x(1).split(","))
    val stockWords2 = industryFile.map(_.split("\t")).flatMap(x => x(1).split(",")).distinct()
    val stockWords3 = sectionFile.map(_.split("\t")).flatMap(x => x(1).split(",")).distinct()
    val stockWord = stockWords1.union(stockWords2).union(stockWords3).distinct()

    //5.2 统计每篇文章出现三类实体词库的次数
    val articles = segWord
    val arr1 = industryWords.collect
    val arr2 = sectionWords.collect
    val arr3 = stockWord.collect

    val newsStat = articles.map(x => {
      val news = x._2
      val article = news
      var j = 0
      var p = 0
      var q = 0
      for (i <- arr1) {
        if (news.contains(i)) {
          j = j + 1
        }
      }
      for (i <- arr2) {
        if (news.contains(i)) {
          p = p + 1
        }
      }
      for (i <- arr3) {
        if (news.contains(i)) {
          q = q + 1
        }
      }
      j + "\t" + p + "\t" + q + "\t" + x._1 +"\t" + news
    })

    //5.3 过滤掉没有出现实体词的文章,剩余为有金融价值的文章。格式为：(url,title)
    val newsStatFilter = newsStat.map(_.split("\t")).filter(x => x(0).toDouble > 0 || x(1).toDouble > 0 || x(2).toDouble > 0)
      .map(x => (x(3),x(4).split("111111")(0))).filter(x => x._1 != null && x._2 != null)

    /**
      * 6. 为每个标题词匹配行业等属性，并且根据idf值提取出最关键的前两个词
      */
    val wordAndProperty = newsStatFilter.join(table2).map(x => (x._2._1,x._2._2)).filter(x => x._1 != null && x._2 != null)

    val topWord = wordAndProperty.map(x => {
      val part2 = x._2
      val part1 =
        try {
          x._1.split(",").filter(x => x.length > 1)
            .filter(x => !x.matches(".*[0-9]+.*"))
            .filter(x => x != "")
            .filter(x => x != " ")
            .filter(x => x != null)
            .map(x => (x, idfs(x)))
        } catch {
        case e: Exception =>
            null
        }

      var topWords:Array[String] = null
      if (part1.length == 1) {
        topWords = part1.map(x => x._1)
      } else if (part1.length >1) {
        topWords = part1.sortBy(x => x._2).takeRight(2).map(x => x._1)
      }

      part2 + "\t" + topWords.filter(x => x != null).mkString(",")

    })
    topWord.cache()

    /**
      * 7. 处理格式，将所有记录转换为 如(se_xxx,set(word1,word2...))格式
      *
      */
    val word1 = topWord.map(_.split("\t")).map(x => (x(0),x(1).split(",")(0)))
    val word2 = topWord.map(_.split("\t")).map(x => (x(0),x(1).split(",")(1)))
    val word = word1.union(word2).union(specialWord).filter(x => x._1.split(" ").length == 3)
    word.cache()

    //股票词库
    val stockKeyWord =  processWord(word.map(x=>(x._1.split(" ")(0),x._2)).reduceByKey((a,b) => a + " " + b),"st_")

    //行业词库
    val industryKeyWord = processWord(word.map(x=>(x._1.split(" ")(1),x._2)).reduceByKey((a,b) => a + " " + b),"in_")

    //概念词库
    val sectionKeyWord = processWord(word.map(x=>(x._1.split(" ")(2),x._2)).reduceByKey((a,b) => a + " " + b),"se_")

    //所有词库合并
    val keyWord = stockKeyWord.union(industryKeyWord).union(sectionKeyWord)
    keyWord.coalesce(1).saveAsTextFile("/user/wangcao/eventWord")

    table1.unpersist()
    table2.unpersist()
    title.unpersist()
    segWord.unpersist()
    topWord.unpersist()
    word.unpersist()

    sc.stop()

  }

}
