package test

import java.io.ByteArrayOutputStream
import java.util.Random
import java.util.zip.DeflaterOutputStream


/**
  * Created by C.J.YOU on 2016/3/7.
  */

object DataAnalysis extends Serializable{

  private def sign(str:String):String ={
    zlibZip(str)
  }

  private def formatString(arr: Array[String]): String = {
    arr(0) + "\t"+ arr(1) + "\t" + arr(2) + "\t" + arr(3) + "\t" + arr(4) + "\t" + arr(5) + "\t" + arr(6) + "\t" + arr(7)
  }
  private def toJson2(arr: Array[String]): String = {
    val json = "{\"id\":\"%s\", \"value\":\"%s\"}"
    json.format(arr(0), arr(1).replace("\r\n","-<"))
  }

  def extractorUrl(str:String): String ={
    var result = ""
    var regexUrl = "weibo.cn"
    val lineSplit = str.split("\t")
    try {

      if (lineSplit.length == 12) {
        val firstSegment = if (lineSplit(0).isEmpty) "NoDef" else lineSplit(0)
        val secondSegment = if (lineSplit(1).isEmpty) "NoDef" else lineSplit(1)
        val thirdSegment = if (lineSplit(2).isEmpty) "NoDef" else lineSplit(2)
        val fourthSegment = if (lineSplit(3).isEmpty) "NoDef" else lineSplit(3)
        val fifthSegment = if (lineSplit(4).isEmpty) "NoDef" else lineSplit(4)
        val eighthSegment = if (lineSplit(7).isEmpty) "NoDef" else lineSplit(7)
        val ninethSegment = if (lineSplit(8).isEmpty) "NoDef" else lineSplit(8)
        val elevethSegment = if (lineSplit(10).isEmpty) "NoDef" else lineSplit(10)
        val newSeg = Array(firstSegment, secondSegment, thirdSegment, fourthSegment, fifthSegment, eighthSegment, ninethSegment,elevethSegment)
        if (newSeg(2) == "NoDef") {
          result = "NoDef"
        } else {
          if((newSeg(2) + newSeg(3)).contains("live.sina.com.cn/zt/f/v/finance/globalnews1") || newSeg(2).contains("180.97.93.28")
            || newSeg(2).contains("61.144.227.239") || newSeg(2).contains("117.22.252.216")
            || newSeg(2).contains("61.154.18.10") || newSeg(2).contains("59.151.61.116")
            || newSeg(2).contains("203.207.93.61") || newSeg(2).contains("203.207.219.20")
            || newSeg(2).contains("119.40.50.158") || newSeg(2).contains("122.224.75.236")
            || newSeg(2).contains("202.113.216.9") || newSeg(2).contains("59.252.162.99"))
            result = sign(formatString(newSeg))
         /* else if (newSeg(2).contains("180.97.93.28"))
            result = sign(formatString(newSeg))
          else if (newSeg(2).contains("61.144.227.239"))
            result = sign(formatString(newSeg))
          else if (newSeg(2).contains("117.22.252.216"))
            result = sign(formatString(newSeg))
          else if (newSeg(2).contains("61.154.18.10"))
            result = sign(formatString(newSeg))
          else if (newSeg(2).contains("59.151.61.116"))
            result = sign(formatString(newSeg))
          else if (newSeg(2).contains("203.207.93.61"))
            result = sign(formatString(newSeg))
          else if (newSeg(2).contains("203.207.219.20"))
            result = sign(formatString(newSeg))
          else if (newSeg(2).contains("119.40.50.158"))
            result = sign(formatString(newSeg))
          else if (newSeg(2).contains("122.224.75.236"))
            result = sign(formatString(newSeg))
          else if (newSeg(2).contains("202.113.216.9"))
            result = sign(formatString(newSeg))
          else if (newSeg(2).contains("59.252.162.99"))
            result = sign(formatString(newSeg))*/
          else {
            result = sign(formatString(newSeg))
            if (newSeg(2).contains("weibo.com") || newSeg(2).contains("weibo.cn") || newSeg(2).contains("qzone.qq.com")) {
              regexUrl = "weibo.cn"
            } else {
              val targetUrl = newSeg(2).replace("www.", "").replace("https://", "").replace("http://", "")
              regexUrl = targetUrl
            }
          }
        }
      }
    } catch {
        case e:Exception =>
    }
    val value = result
    val key = if (lineSplit(10).isEmpty) "NoDef" else lineSplit(10)
    println("start:"+key )
    if (result.nonEmpty) {
      val arr = Array[String](new Random().nextInt(500).toString, value)
      val ret_str = toJson2(arr)
      if (ret_str.length() > 6959)
        "TOOBIGVALUE_KUNYAN" + "\t" + "TBV"
      else
        regexUrl + "\t" + ret_str
    } else null
  }

  private def  zlibZip(primStr:String):String = {
    if (primStr == null || primStr.length () == 0) {
      return primStr
    }
    val out = new ByteArrayOutputStream ()
    val gzip = new DeflaterOutputStream (out)
    try {
      gzip.write (primStr.getBytes())
    } catch {

      case e: Exception => e.printStackTrace ();
    } finally {
      if (gzip != null) {
        try {
          gzip.close ()
        } catch {
          case e: Exception => e.printStackTrace ();
        }
      }
    }
    new sun.misc.BASE64Encoder().encode(out.toByteArray)
  }

  def main (args: Array[String]) {
    val url = "a\t163\thttp://www.baidu.com.hk\t1633\t1633\t1363\t1633\t1633\t1633\t1633\t1633\t"
    println(extractorUrl(url).split("\t")(0))
  }
}
