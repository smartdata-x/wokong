package kunyandata

/**
  * Created by lenovo on 2016/2/16.
  */
import java.io._
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkConf, SparkContext}
import org.wltea.analyzer.IKSegmentation
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source


  object HbaseSpark {
  val mapAfter = new mutable.HashMap[String, Int]()
  val mapBefore = new mutable.HashMap[String, Int]()
  val mapDiff = new mutable.HashMap[String, Int]()
  val mapimp = new mutable.HashMap[String, Array[String]]()
  val mapindustry = new mutable.HashMap[String, String]()
  val mapstock_code = new mutable.HashMap[String, String]()
  val mapsection = new mutable.HashMap[String, String]()
  val mapstockk = new mutable.HashMap[String, String]()
  val conf = HBaseConfiguration.create()
  val sparkConf = new SparkConf().setAppName("hotword_shj")
  val sparkContext = new SparkContext(sparkConf)
  //  var listKey = new ListBuffer[String]()

  //得到hbase里的数据并作为RDD
  def obtainHbaseRdd(): Unit = {

  println("after config")

  getTime()
  init("6_analyzed")
  val hbaseRdd = sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat]
   , classOf[ImmutableBytesWritable], classOf[Result])
  init("5_analyzed")
  val hbaseRdd1 = sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat]
    , classOf[ImmutableBytesWritable], classOf[Result])
  init("3_analyzed")
  val hbaseRdd2 = sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat]
    , classOf[ImmutableBytesWritable], classOf[Result])

  val rdd=hbaseRdd.++(hbaseRdd1)
  val rdd1=rdd.++(hbaseRdd2)

  //排序+topn
  val array = rdd1.flatMap(SplitWords(_)).map((_, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(false).top(200)
  array.foreach(println)

  println("after hbase")

  //获取当前小时的时间戳 并以此时间戳命名文件夹
  val dataafter = new Date(new Date().getTime)
  val sdf = new SimpleDateFormat("yyyy-MM-dd-HH")
  val strafter = sdf.format(dataafter)
  println(strafter)

  val fileafter = new File("/home/shj/" + strafter)

  //将当前小时的数据写进mapafter集合中 并写进以当前小时时间戳命名的本地文件中
  var arraya = ""
  var i = 1
  for (a <- array) {

   arraya += a._2.toString
   arraya += "\t"
   arraya += i.toString
   i += 1
   arraya += "\n"

   val writer = new PrintWriter(fileafter,"UTF-8")
   writer.write(arraya)
   writer.flush()
   writer.close()

   }
   //将数据写进mapafter中
   val line = arraya.split("\n")
   for (i <- 0 until line.length - 1) {
   val keyValue = line(i).trim
   val r = keyValue.split("\t")
   if (arraya.length > 0) {
   for (x <- 0 until r.length - 1) {

     mapAfter.+=((r(0), r(1).toInt))
     }
     }
     }

   try {

   //从本地文件读取上个小时的数据 并将结果写入map集合中
   // 根据时间戳作为上一小时的文件名
   val databefore = new Date(new Date().getTime - 60 * 60 * 1000)
   val sdf = new SimpleDateFormat("yyyy-MM-dd-HH")
   val strbefore = sdf.format(databefore)
   val fbefore = new File("/home/shj/" + strbefore)
   if(fbefore.exists()) {

    Source.fromFile(fbefore,"UTF-8").getLines().foreach(keyValue => {

    val r = keyValue.split("\t")
    mapBefore.+=((r(0), r(1).toInt))

    })
   }else{

   fbefore.createNewFile()
   println("上一小时无数据")
     }
   //将最后处理的排序差结果放到mapDiff集合中
   val mapiterator = mapAfter.iterator
   var difference = 0
   var value: Int = 0
   var key = ""

   while (mapiterator.hasNext) {

   val keyWord = mapiterator.next()
   key = keyWord._1
   value = keyWord._2
   //判断前后两小时的数据还有有交融的
   if (mapBefore.contains(key)) {

   difference = value - mapBefore.get(key).get
   mapDiff.+=((key, difference))
   } else {

   val differences = value - mapBefore.size
   mapDiff.+=((key, differences))

   }
   //遍历mapDiff 并将keydiff封装到arrKey数组中
   var listKey = new ListBuffer[String]()
   //输出排序差在前100的数据
   sparkContext.parallelize(mapDiff.toSeq).map(x => (x._2, x._1)).sortByKey(true).takeOrdered(100).foreach(x=>{

   listKey+=x._2

    })
    //getIndustry_words
    getIndustry_words(listKey)

    //调用getsection_words
    getsection_words(listKey)

    //从本地获取股票关键字词典 并放入mapimp集合中

    val filestockk=new File("/home/shj/stock.txt")
    Source.fromFile(filestockk, "UTF-8").getLines().foreach(keyValuestock => {
    val arr = keyValuestock.split("\t")
    val r1= arr(0)
    val name = arr(1)
    val code =r1.split("\\.")(0)
    mapstockk.+=(name-> code)

    })
    val fileimp2 = new File("/home/shj/stock_words.words")
    // println("fileimp2:"+fileimp)
    Source.fromFile(fileimp2, "UTF-8").getLines().foreach(keyValueimp => {

    val r = keyValueimp.split("\t")
    val keyimp = r(0)
    val valueimp = r(1).split(",")
    mapimp.+=(keyimp -> valueimp)
    val arrKey = listKey.toArray
    val arr =arrKey.iterator
    while(arr.hasNext) {
    val arrkey = arr.next()

    //判断 valueimp中是否包含arrKey
    if (valueimp.contains(arrkey)) {

    mapstock_code.+=(arrkey->mapstockk.get(keyimp).get)

    } else {
    }
     }
    })
     }
    } catch {
    case e: Exception => e.printStackTrace()
        sparkContext.stop()
    }

    compare

    println(mapindustry.mkString("\t"))
    println("____________________")
    println(mapstock_code.mkString("\t"))
    println("____________________")
    println(mapsection.mkString("\t"))
    println("____________________")
    println("_____________________________________THE END")

    sparkContext.stop()
    System.exit(0)
     }

    def compare(): Unit = {

    var paramaMap = new mutable.HashMap[String, String]
    var paramaMapp = new mutable.HashMap[String, String]

    mapindustry.foreach(x => {
    val hot_word = x._1
    val industry = x._2

    if (hot_word != null) {

    paramaMap.+=("hot_words" -> hot_word)
      }
    paramaMap.+=("hot_words" -> hot_word, "industry" -> industry)
    HotWordHttp.sendNew("http://120.55.189.211/cgi-bin/northsea/prsim/subscribe/1/hot_words_notice.fcgi", paramaMap)
    if (industry != null) {

    paramaMap.+=("industry" -> industry)
    }
    paramaMap.+=("hot_words" -> hot_word, "industry" -> industry)
    HotWordHttp.sendNew("http://120.55.189.211/cgi-bin/northsea/prsim/subscribe/1/hot_words_notice.fcgi", paramaMap)
    })

    mapsection.foreach(x => {

    val hot_word = x._1
    val section = x._2

    if (hot_word != null) {
    paramaMap.+=("hot_words" -> hot_word)
     }
    paramaMap.+=("hot_words" -> hot_word, "section" -> section)
    HotWordHttp.sendNew("http://120.55.189.211/cgi-bin/northsea/prsim/subscribe/1/hot_words_notice.fcgi", paramaMap)
    if (section != null) {

    paramaMap.+=("section" -> section)
    }
    paramaMap.+=("hot_words" -> hot_word, "section" -> section)
    HotWordHttp.sendNew("http://120.55.189.211/cgi-bin/northsea/prsim/subscribe/1/hot_words_notice.fcgi", paramaMap)
    })

    mapstock_code.foreach(x => {
    val hot_word = x._1
    val stock_code = x._2

    if (hot_word != null) {
    paramaMapp.+=("hot_words" -> hot_word)
    }
    paramaMapp.+=("hot_words" -> hot_word, "stock_code" -> stock_code)
    HotWordHttp.sendNew("http://120.55.189.211/cgi-bin/northsea/prsim/subscribe/1/hot_words_notice.fcgi", paramaMapp)
    if (stock_code != null) {

    paramaMapp.+=("stock_code" -> stock_code)
    }
    paramaMapp.+=("hot_words" -> hot_word, "stock_code" -> stock_code)
    HotWordHttp.sendNew("http://120.55.189.211/cgi-bin/northsea/prsim/subscribe/1/hot_words_notice.fcgi", paramaMapp)
    })
    }

    /**
     * 分词的方法
     **/
    def SplitWords(tuple: (ImmutableBytesWritable, Result)): mutable.MutableList[String] = {

    var list = new mutable.MutableList[String]
    val result = tuple._2
    val title = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("title")))
    val reader: StringReader = new StringReader(title)
    val ik = new IKSegmentation(reader, true)
    var lexeme = ik.next()
    while (lexeme != null) {

    val word = lexeme.getLexemeText()
    list += word.toString
    lexeme = ik.next()
    }

    list
    }

    //获取时间戳
    def getTime(): Unit ={

    val scan = new Scan()
    val date = new Date(new Date().getTime - 60 * 60 * 1000)
    val format = new SimpleDateFormat("yyyy-MM-dd HH")
    val time = format.format(date)
    val time1 = format.format(new Date().getTime)
    val starttime = time + "-00-00"
    val stoptime = time1 + "-00-00"
    val sdf1: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
    val startrow: Long = sdf1.parse(starttime).getTime
    val stoprow: Long = sdf1.parse(stoptime).getTime
    println(startrow)
    println(stoprow)
    scan.setTimeRange(startrow, stoprow)
    val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)
    conf.set(TableInputFormat.SCAN, scanToString)
     }

    //从本地获取行业关键字词典 并放入mapimp集合中
    def getIndustry_words(listKey:ListBuffer[String]): Unit ={
    val arrKey = listKey.toArray
    val fileimp1 = new File("/home/shj/industry_words1.words")
    //println("fileimp1:"+fileimp)
    Source.fromFile(fileimp1, "UTF-8").getLines().foreach(keyValueimp => {

    val r = keyValueimp.split("\t")
    val keyimp = r(0)
    val valueimp = r(1).split(",")
    mapimp.+=(keyimp -> valueimp)

    val arr =arrKey.iterator
    while(arr.hasNext) {

    val arrkey = arr.next()
    //判断 valueimp中是否包含arrKey
    if (valueimp.contains(arrkey)) {

    mapsection.+=(arrkey -> keyimp)

    } else {

    }
    }
    })
    }

    //从本地获取板块关键字词典 并放入mapimp集合中
    def getsection_words(listKey:ListBuffer[String]): Unit ={

    val arrKey = listKey.toArray
    val fileimp1 = new File("/home/shj/section_words.words")
    //println("fileimp1:"+fileimp)
    Source.fromFile(fileimp1, "UTF-8").getLines().foreach(keyValueimp => {

    val r = keyValueimp.split("\t")
    val keyimp = r(0)
    val valueimp = r(1).split(",")
    mapimp.+=(keyimp -> valueimp)

    val arr =arrKey.iterator
    while(arr.hasNext) {
    val arrkey = arr.next()

    //判断 valueimp中是否包含arrKey
    if (valueimp.contains(arrkey)) {

    mapsection.+=(arrkey -> keyimp)

    } else {

    }
     }
    })
    }

    def init(tb:String): Unit ={

    conf.set("hbase.rootdir", "hdfs://ns1/hbase")
    conf.set("hbase.zookeeper.quorum", "server0,server1,server2")
    conf.set(TableInputFormat.INPUT_TABLE, tb)
    //    conf.set(TableInputFormat.INPUT_TABLE, "3_analyzed")
    //    conf.set(TableInputFormat.INPUT_TABLE, "6_analyzed")
    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "info")
    conf.set(TableInputFormat.SCAN_COLUMNS, "title")

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    }


    def main(args: Array[String]) {

    obtainHbaseRdd()
    }
    }




