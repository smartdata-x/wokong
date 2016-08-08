import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import com.kunyan.MailMain
import com.kunyan.search.KafkaProducer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by lcm on 2016/7/7.
  * search 数据解析的主类
  */

object SparkDriver {

  val EXPIRE_DAY = "1"

  /**
    * 发送数据到kafka
    * @param table kv表名
    * @param key 获取kv数据的key值
    * @param value kv的value
    */
  def sendToKafka(table:String, key:String, value: String): Unit = {

    KafkaProducer.send(table + "\001" + EXPIRE_DAY + "\001" + key + "\001" + value)

  }

  /**
    * 生成kv表中的key
    * @param ts 原始数据中的时间戳
    * @return 返回key
    */
  def getKey(ts: String): String = {

    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val time = ts.toLong
    val dt = new Date(time)
    val dateTime = sdf.format(dt)

    dateTime

  }

  def main(args: Array[String]): Unit = {


    System.setProperty("spark.shuffle.consolidateFiles", "true")
    System.setProperty("spark.speculation", "true")
    System.setProperty("spark.streaming.concurrentJobs", "10")
    System.setProperty("spark.yarn.max.executor.failures", "99")
    System.setProperty("spark.streaming.blockInterval", "100")
    System.setProperty("spark.akka.timeout", "300")
    System.setProperty("spark.rpc.numRetries", "6")
    System.setProperty("spark.executor.heartbeatInterval", "20")
    System.setProperty("spark.task.maxFailures", "8")
    System.setProperty("spark.network.timeout", "1000")

    val ssc = new StreamingContext(new SparkConf(), Seconds(60))
    val sc  = ssc.sparkContext

    val Array(dataDir, table, mailTopic, mailContent, mailList) = args

    val visitSearchPatterns = Array[String](
     // 搜索
      "quotes.money.163.com.*word=(.*)&t=", "quotes.money.163.com.*word=(.*)&count",
      "suggest3.sinajs.cn.*key=(((?!&name).)*)", "xueqiu.com.*code=(.*)&size", "xueqiu.com.*q=(.*)",
      "app.stcn.com.*&wd=(.*)", "q.ssajax.cn.*q=(.*)&type", "znsv.baidu.com.*wd=(.*)&ch",
      "so.stockstar.com.*q=(.*)&click", "quote.cfi.cn.*keyword=(.*)&his", "hq.cnstock.com.*q=(.*)&limit",
      "xinpi.cs.com.cn.*q=(.*)", "xinpi.cs.com.cn.*c=(.*)&q", "search.cs.com.cn.*searchword=(.*)",
      "d.10jqka.com.cn.*(\\d{6})/last", "news.10jqka.com.cn.*keyboard_(\\d{6})",
      "10jqka.com.cn.*w=(.*)&tid", "data.10jqka.com.cn.*/(\\d{6})/", "dict.hexin.cn.*pattern=(\\d{6})",
      "dict.hexin.cn.*pattern=(\\S.*)", "q.stock.sohu.com.*keyword=(.*)&(_|c)",
      "api.k.sohu.com.*words=(.*)&p1", "hq.p5w.net.*query=(.*)&i=", "code.jrjimg.cn.*key=(.*)&d=",
      "itougu.jrj.com.cn.*keyword=(.*)&is_stock", "wallstreetcn.com/search\\?q=(.*)",
      "api.markets.wallstreetcn.com.*q=(.*)&limit", "so.hexun.com.*key=(.*)&type",
      "finance.ifeng.com.*code.*(\\d{6})", "finance.ifeng.com.*q=(.*)&cb",
      "suggest.eastmoney.com.*input=(.*?)&", "so.eastmoney.com.*q=(.*)&m",
      "guba.eastmoney.com/search.aspx?t=(.*)", "www.yicai.com.*searchKeyWords=(.*)",
      "www.gw.com.cn.*q=(.*)&s=", "cailianpress.com/search.*keyword=(.*)",
      "api.cailianpress.com.*PutinInfo=(.*)&sign", "news.21cn.com.*keywords=(.*)&view",
      "news.10jqka.com.cn.*text=(.*)&jsoncallback", "smartbox.gtimg.cn.*q=(.*?)&",
      "swww.niuguwang.com/stock/.*q=(.*?)&",
      // 查看
      "quotes.money.163.com/app/stock/\\d(\\d{6})", "m.news.so.com.*q=(\\d{6})",
      "finance.sina.com.cn.*(\\d{6})/nc.shtml", "guba.sina.com.cn.*name=.*(\\d{6})",
      "platform.*symbol=\\w\\w(\\d{6})", "xueqiu.com/S/.*(\\d{6})", "cy.stcn.com/S/.*(\\d{6})",
      "mobile.stcn.com.*secucode=(\\d{6})", "stock.quote.stockstar.com/(\\d{6}).shtml",
      "quote.cfi.cn.*searchcode=(.*)", "hqapi.gxfin.com.*code=(\\d{6}).sh",
      "irm.cnstock.com.*index/(\\d{6})", "app.cnstock.com.*k=(\\d{6})",
      "guba.eastmoney.com.*code=.*(\\d{6})", "stockpage.10jqka.com.cn/(\\d{6})/",
      "0033.*list.*(\\d{6}).*json", "q.stock.sohu.com/cn/(\\d{6})/", "s.m.sohu.com.*/(\\d{6})",
      "data.p5w.net.*code.*(\\d{6})", "hq.p5w.net.*a=.*(\\d{6})", "jrj.com.cn.*(\\d{6}).s?html",
      "mapi.jrj.com.cn.*stockcode=(\\d{6})", "api.buzz.wallstreetcn.com.*(\\d{6})&cid",
      "stockdata.stock.hexun.com/(\\d{6}).shtml", "finance.ifeng.com/app/hq/stock.*(\\d{6})",
      "api.3g.ifeng.com.*k=(\\d{6})", "quote.*(\\d{6}).html", "gw.*stock.*?(\\d{6})",
      "tfile.*(\\d{6})/fs_remind", "api.cailianpress.com.*(\\d{6})&Sing",
      "stock.caijing.com.cn.*(\\d{6}).html", "gu.qq.com.*(\\d{6})\\?", "gubaapi.*code=.*(\\d{6})",
      "mnews.gw.com.cn.*(\\d{6})/(list|gsgg|gsxw|yjbg)", "101.226.68.82.*code=.*(\\d{6})",
      "183.238.123.235.*code=(\\d{6})&", "zszx.newone.com.cn.*code=(\\d{6})",
      "compinfo.hsmdb.com.*stock_code=(\\d{6})", "mt.emoney.cn.*barid=(\\d{6})")

    try {

      sc.textFile(dataDir).filter(_.split("\t").length > 7 ).map { data =>

        var key = ""
        var value = ""
        var stockCode = ""

        val dataArr = data.split("\t")
        val ts = dataArr(2)
        val url = dataArr(3)

        for (i <- visitSearchPatterns.indices) {

          val matcher = Pattern.compile(visitSearchPatterns(i)).matcher(url)

          if (matcher.find()) {

            stockCode = matcher.group(1)
            key = getKey(ts)
            value = stockCode + "\t" + ts + "\t" + i

          }
        }

        (key, value)

      }.filter(x => x._1.trim != "" && x._2.length <= 6000).combineByKey(
        (v: String) => List(v),
        (c: List[String], v: String) => v :: c,
        (c1: List[String], c2: List[String]) => c1 ::: c2)
        .foreach { x =>
          x._2.zipWithIndex.foreach { y =>
            sendToKafka(table, x._1 + y._2, y._1)
        }
      }
    }  catch {
       case e: Exception => 
	   SUELogger.exception(e)
       MailMain.sendMail(Array(mailTopic, mailContent + e.getMessage, mailList))
    }
  }

}
