package com.kunyan.telecom
import java.io.ByteArrayOutputStream
import java.util.Random
import java.util.zip.DeflaterOutputStream


/**
  * Created by C.J.YOU on 2016/3/7.
  * 电信原始数据处理类
  */

object DataAnalysis extends Serializable {

  /**
    * 数据压缩
    * @param str 原始数据
    * @return 压缩后的数据
    */
  private def sign(str:String): String = {

    zlibZip(str)

  }

  /**
    * 数据格式化
    * @param arr 字符数组
    * @return 格式化后的字符串
    */
  private def formatString(arr: Array[String]): String = {

    arr(0) + "\t"+ arr(1) + "\t" + arr(2) + "\t" + arr(3) + "\t" + arr(4) + "\t" + arr(5) + "\t" + arr(6)

  }

  /**
    * json封装
    * @param arr 字符数组
    * @return 返回封装后的字符串
    */
  private def toJson2(arr: Array[String]): String = {

    val json = "{\"id\":\"%s\", \"value\":\"%s\"}"
    val jsonString = json.format(arr(0), arr(1).replace("\n","-<"))

    jsonString +"_kunyan_" + arr(2)

  }

  /**
    * 数据解析
    * @param str 源数据
    * @return 解析后数据
    */
  def extractorUrl(str:String): String = {

    var result = ""
    var regexUrl = "weibo.cn"
    var keyword = ""

    try {

      val lineSplit = str.split("\t")

      if (lineSplit.length == 12) {

        val firstSegment = if (lineSplit(0).isEmpty) "NoDef" else lineSplit(0)
        val secondSegment = if (lineSplit(1).isEmpty) "NoDef" else lineSplit(1)
        val thirdSegment = if (lineSplit(2).isEmpty) "NoDef" else lineSplit(2)
        val fourthSegment = if (lineSplit(3).isEmpty) "NoDef" else lineSplit(3)
        val fifthSegment = if (lineSplit(4).isEmpty) "NoDef" else lineSplit(4)
        val eighthSegment = if (lineSplit(7).isEmpty) "NoDef" else lineSplit(7)
        val ninethSegment = if (lineSplit(8).isEmpty) "NoDef" else lineSplit(8)
        val elevethSegment = if (lineSplit(10).isEmpty) "NoDef" else lineSplit(10)
        keyword = elevethSegment

        val newSeg = Array(firstSegment, secondSegment, thirdSegment, fourthSegment, fifthSegment, eighthSegment, ninethSegment,elevethSegment)

        if (newSeg(2) == "NoDef") {
          result = "NoDef"
        } else {

          if((newSeg(2) + newSeg(3)).contains("live.sina.com.cn/zt/f/v/finance/globalnews1") || newSeg(2).contains("180.97.93.28")
            || newSeg(2).contains("61.144.227.239") || newSeg(2).contains("117.22.252.216")
            || newSeg(2).contains("61.154.18.10") || newSeg(2).contains("59.151.61.116")  || newSeg(2).contains("meituan.com")
            || newSeg(2).contains("203.207.93.61") || newSeg(2).contains("203.207.219.20") || newSeg(2).contains("linkedin.com")
            || newSeg(2).contains("119.40.50.158") || newSeg(2).contains("122.224.75.236")|| newSeg(2).contains("maimai.cn")
            || newSeg(2).contains("202.113.216.9") || newSeg(2).contains("59.252.162.99") || newSeg(2).contains("weibo.com")
            || newSeg(2).contains("weibo.cn") || newSeg(2).contains("qzone.qq.com") || newSeg(2).contains("ele.me") || newSeg(2).contains("maimai.com")
            || newSeg(2).contains("pailequ.cn") || newSeg(2).contains("beequick.cn") || newSeg(2).contains("waimai"))

            result = sign(formatString(newSeg))

          else {

            result = sign(formatString(newSeg))
            val targetUrl = newSeg(2).replace("www.", "").replace("https://", "").replace("http://", "")
            regexUrl = targetUrl

          }
        }
      }

    } catch {
        case e:Exception => SUELogger.exception(e)
    }

    val value = result

    if (result.nonEmpty) {

      val arr = Array[String](new Random().nextInt(500).toString,value,keyword)
      val ret_str = toJson2(arr)

      if (ret_str.length() > 6959)
        "TOOBIGVALUE_KUNYAN" + "\t" + "TBV"
      else
        regexUrl + "\t" + ret_str

    } else null

  }

  /**
    * 压缩算法
    * @param primStr 原始值
    * @return 压缩后值
    */
  private def  zlibZip(primStr: String): String = {

    if (primStr == null || primStr.length () == 0) {
      return primStr
    }

    val out = new ByteArrayOutputStream ()
    val gzip = new DeflaterOutputStream (out)

    try {
      gzip.write (primStr.getBytes())
    } catch {
      case e: Exception => SUELogger.exception(e)
    } finally {

      if (gzip != null) {

        try {
          gzip.close ()
        } catch {
          case e: Exception => SUELogger.exception(e)
        }
      }
    }

    new sun.misc.BASE64Encoder().encode(out.toByteArray)

  }
}
