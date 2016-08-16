package request

import config.TelecomConfig
import org.json.JSONObject
import org.jsoup.Jsoup
import sun.misc.BASE64Decoder

/**
  * Created by C.J.YOU on 2016/8/12.
  * 向kv请求数据
  */
object Request {


  def getValue(key: String): String = {

    var value = ""

    val url = "http://61.129.39.71/telecom-dmp/kv/getValueByKey?token=" + Token.token() + "&table="+TelecomConfig.TABLE  + "&key=" + key

    println("url:" + url)

    try {
      val result = new JSONObject(Jsoup.connect(url).timeout(5000).execute().body()).get("result").toString

      if (result != "null") {
        value = getFromBASE64(new JSONObject(result).get("value").toString)
         println("value:" + value)
      }
    } catch {
      case e:Exception => println(e.getMessage)
    }

    value


  }


  /**
    * 解码Base64字符串
    *
    * @param str base64字符串
    * @return 解码后的字符串
    */
  def getFromBASE64(str: String): String = {

    if (str == null)
      return null

    val decoder = new BASE64Decoder()

    try {

      val bytes = decoder.decodeBuffer(str)
      new String(bytes);

    } catch {
      case e:Exception => println(e.getMessage)
      null

    }
  }



}
