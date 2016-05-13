package com.kunyan.util

import java.text.SimpleDateFormat
import java.util.Date
import com.ibm.icu.text.CharsetDetector
import com.kunyan.config.SentimentConf
import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing}
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
import scala.collection.mutable.ArrayBuffer

/**
  * Created by WangCao on 2016/4/22
  * 金融事件词库的建立
  */
object EventLibrary {

  val TABLE_PREFIX = List[Int](3, 5, 6, 7, 8, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25)
  val hbaseConf = HBaseConfiguration.create()
  val sparkConf = new SparkConf().setAppName("EventLibrary").setMaster("local")
  val sc = new SparkContext(sparkConf)


  /**
    *配置hbase接口
    */
  def getHbaseConf: Configuration = {

    hbaseConf.set("hbase.rootdir", "hdfs://master:9000/hbase")
    hbaseConf.set("hbase.zookeeper.quorum", "master,slave1,slave2")

    hbaseConf

  }

  /**
    * 判断字符编码
    *
    * @param html 待识别编码的文本
    * @return 字符编码
    */
  def judgeCharset(html: Array[Byte]): String = {

    val icu4j = new CharsetDetector()
    icu4j.setText(html)
    val encoding = icu4j.detect()

    encoding.getName

  }

  /**
    * 设置读取hbase表格的时间范围
    */
  def setTimeRange(): Unit = {

    val scan = new Scan()
    val date = new Date(new Date().getTime - 60 * 60 * 1000 * 48)
    val format = new SimpleDateFormat("yyyy-MM-dd HH")
    val startTime = format.format(date) + "-00-00"
    val stopTime = format.format(new Date().getTime) + "-00-00"
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
    val startRow: Long = sdf.parse(startTime).getTime
    val stopRow: Long = sdf.parse(stopTime).getTime

    scan.setTimeRange(startRow, stopRow)
    val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)
    hbaseConf.set(TableInputFormat.SCAN, scanToString)

  }

  /**
    * 获取hbase中的表格并存储成HbaseRDD
    *
    * @param tableName 欲获取的hbase中的表格的名字
    * @return 读取后的hbaseRdd
    */
  def getHbaseRdd(tableName: String): RDD[(ImmutableBytesWritable, Result)] = {

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val hbaseConf = getHbaseConf
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

    setTimeRange()

    val hbaseRdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable], classOf[Result])

    hbaseRdd

  }

  /**
    * 读取第一类表格的数据：url+title+content
    *
    * @return 新闻链接，标题，内容
    */

  def getContentTable: RDD[String] = {

    val news = getHbaseRdd("wk_detail").map(x => {

      val url = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("url"))
      val title = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("title"))
      val content = x._2.getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))

      val formatUrl = judgeCharset(url)
      val formatTitle = judgeCharset(title)
      val formatContent = judgeCharset(content)
      new String(url, formatUrl) + "\t" + new String(title, formatTitle) + "\t" + new String(content, formatContent)
        .replaceAll("\\&[a-zA-Z]{1,10};", "")
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

  /**
    * 读取第二类表格的数据：url+category+industry+section
    *
    * @return 新闻链接与新闻属性
    */
  def getPropertyTable: RDD[(String, String)] = {

    var rddUnion = getHbaseRdd(TABLE_PREFIX.head + "_analyzed")

    TABLE_PREFIX.slice(1, TABLE_PREFIX.size).foreach(x => {
      val tableName = x + "_analyzed"
      val tempRdd = getHbaseRdd(tableName)
      rddUnion = rddUnion.union(tempRdd)
    })

    val news = rddUnion.map(x => {

      val url = x._2.getRow
      val stock = x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("category"))
      val industry = x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("industry"))
      val section = x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("section"))

      val formatUrl = judgeCharset(url)
      val formatStock = judgeCharset(stock)
      val formatIndustry = judgeCharset(industry)
      val formatSection = judgeCharset(section)
      new String(url, formatUrl) + "\t" +
        new String(stock, formatStock) + "\t" +
        new String(industry, formatIndustry) + "\t" +
        new String(section, formatSection)

    })

    val processedNews = news.map(_.split("\t")).filter(x => x.length == 4)
      .filter(x => x(1) != "" && x(2) != "" && x(3) != "" )
      .map(x=>(x(0), x(1)+" "+x(2)+" "+x(3)))

    processedNews

  }

  /**
    * 转换数据格式，将数据转换成如：se_xxx,set(word1,word2...)这样的格式
    *
    * @param hotWords 以如(industy,word)形式输入的RDD
    * @param pre 前缀，se_表示section（板块）,in_表示industry(行业）,st_表示stock(股票号)
    * @return
    */
  def processWord (hotWords:RDD[(String,String)],pre:String): RDD[(String,Set[String])] = {

    val arr = new ArrayBuffer[String]()
    val hotWordsList = hotWords.collect

    for (line <- hotWordsList) {

      val list = line._1.split(",").toList

      for (i <- list) {
        arr += pre + i + "\t" + line._2
      }

    }

    val arrRdd = sc.parallelize(arr).map(_.split("\t")).map(x => (x(0), x(1).split(" ").toSet))

    arrRdd

  }

  def main(args: Array[String]): Unit = {

    // 获取配置文件信息
    val configInfo = new SentimentConf()
    configInfo.initConfig(args(0))

    // 配置kunyan分词
    val kunyanConfig = new KunyanConf
    kunyanConfig.set(configInfo.getValue("kunyan", "host"), configInfo.getValue("kunyan", "port").toInt)

    // 1.分别读取hbase的两类表
    //contentTable:  url title content
    //propertyTable:  url category  industry  section

    val contentTable = getContentTable.map(_.split("\t"))
    val propertyTable = getPropertyTable
    contentTable.cache()
    propertyTable.cache()


    //2.筛选出标题中长度为2-8的引号中的词，这些词默认为关键词，
    val specialTitle = contentTable.map(x => (x(0), x(1)))
      .filter(x => x._2.contains("“") && x._2.contains("”"))
    specialTitle.cache()

    val specialWordFirst = specialTitle
      .map(x => {

        val title = x._2
        val word =
          try {
            title.substring(title.indexOf("“")+1, title.indexOf("”"))
          } catch {
            case e: Exception =>
              null
          }

        (x._1, word)

      }).filter(x => x._1 !=  null && x._2 != null)

    val specialWordSecond = specialTitle
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

    val specialWord = specialWordFirst.union(specialWordSecond)
      .filter(x => x._2.length >= 2 && x._2.length <= 8)
      .join(propertyTable).map(x => (x._2._2,x._2._1))


    //3. 标题与正文分词
    //3.1 获取停用词
    val stopWords = sc.textFile(args(1)).collect
    val stopWordsBr = sc.broadcast(stopWords).value

    //3.2调用分词程序
    val segWord = contentTable.map(x => (x(0), x(1) + "111111" + x(2)))
      .map(x => (x._1, TextPreprocessing.process(x._2, stopWordsBr,kunyanConfig).mkString(",")))

    segWord.cache()


    //4.计算IDF值，创建语料库
    //4.1 计算词项频率TF值,取标题与正文
    val totalWords = segWord.map(x => x._2).map(_.replace("111111", "")).map(_.split(",")).map(x => x.toSeq)
    totalWords.count()

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


    //5. 筛选出有金融价值的文章，并获取这些文章的标题
    //5.1 建立三个实体词典：股票，行业，概念
    val industryFile = sc.textFile(args(2))
    val sectionFile = sc.textFile(args(3))
    val stockFile = sc.textFile(args(4))

    val industryWords = industryFile.map(_.split("\t")).map(x => x(0)).distinct()
    val sectionWords = sectionFile.map(_.split("\t")).map(x => x(0)).distinct()
    val stockWordsPartFirst = stockFile.map(_.split("\t")).flatMap(x => x(1).split(","))
    val stockWordsPartSecond = industryFile.map(_.split("\t")).flatMap(x => x(1).split(",")).distinct()
    val stockWordsPartThird = sectionFile.map(_.split("\t")).flatMap(x => x(1).split(",")).distinct()
    val stockWords = stockWordsPartFirst.union(stockWordsPartSecond)
      .union(stockWordsPartThird).distinct()

    //5.2 统计每篇文章出现三类实体词库的次数
    val industyArr = industryWords.collect
    val sectionArr = sectionWords.collect
    val stockArr = stockWords.collect

    val newsStat = segWord.map(x => {
      val news = x._2
      var j = 0
      var p = 0
      var q = 0
      for (i <- industyArr) {
        if (news.contains(i)) {
          j = j + 1
        }
      }
      for (i <- sectionArr) {
        if (news.contains(i)) {
          p = p + 1
        }
      }
      for (i <- stockArr) {
        if (news.contains(i)) {
          q = q + 1
        }
      }
      j + "\t" + p + "\t" + q + "\t" + x._1 +"\t" + news
    })

    //5.3 过滤掉没有出现实体词的文章,剩余为有金融价值的文章,并只保留其标题
    val newsStatFilter = newsStat.map(_.split("\t"))
      .filter(x => x(0).toDouble > 0 || x(1).toDouble > 0 || x(2).toDouble > 0)
      .map(x => (x(3), x(4).split("111111")(0))).filter(x => x._1 != null && x._2 != null)


    //6. 为每个标题词匹配行业等属性，并且根据idf值提取出最关键的前两个词
    val wordAndProperty = newsStatFilter.join(propertyTable).map(x => (x._2._1,x._2._2))
    val idfsKeys = idfs.keys.mkString(",")

    val wordAndidf = wordAndProperty.map(x => {
      val property = x._2
      val words = x._1.split(",").filter(x => x.length > 1)
        .filter(x => !x.matches(".*[0-9]+.*"))
        .filter(x => x != "")
        .filter(x => x != " ")
        .filter(x => x != null)
        .map(x => {
          if (idfsKeys.contains(x)) {
            (x, idfs(x))
          }
          (x, idfs(x))
        })
      (property, words)
    })
    val topWord = wordAndidf.filter(x => x._2.length >= 2)
      .map(x => x._1 + "\t" + x._2.sortBy(a => a._2).takeRight(2).map(_._1).mkString(","))


    //7. 处理格式，将所有记录转换为 如(se_xxx,set(word1,word2...))格式
    val word = topWord.map(_.split("\t")).flatMap(x => {
      Array[(String, String)]((x(0), x(1).split(",")(0)), (x(0), x(1).split(",")(1)))
    }).union(specialWord).filter(x => x._1.split(" ").length == 3)

    //股票词库
    val stockKeyWord =  processWord(word.map(x=>(x._1.split(" ")(0), x._2))
      .reduceByKey((stock,word) => stock + " " + word), "st_")

    //行业词库
    val industryKeyWord = processWord(word.map(x=>(x._1.split(" ")(1), x._2))
      .reduceByKey((industry,word) => industry + " " + word), "in_")

    //概念词库
    val sectionKeyWord = processWord(word.map(x=>(x._1.split(" ")(2), x._2))
      .reduceByKey((section,word) => section + " " + word), "se_")

    //所有词库合并
    val keyWord = stockKeyWord.union(industryKeyWord).union(sectionKeyWord)

    contentTable.unpersist()
    propertyTable.unpersist()
    specialTitle.unpersist()
    segWord.unpersist()

    sc.stop()

  }

}
