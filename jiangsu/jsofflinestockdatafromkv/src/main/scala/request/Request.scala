package request

import config.TelecomConfig
import log.UserLogger
import org.json.JSONObject
import org.jsoup.Jsoup
import sun.misc.BASE64Decoder

/**
  * Created by C.J.YOU on 2016/8/12.
  * 向kv请求数据
  */
object Request {


  /**
    * kv 根据key值请求value数据
    * @param key key
    * @return value
    */
  def getValue(key: String): String = {

    var value = ""

    val token = Token.token()

    if(token.isEmpty)
      return value

    val url = "http://180.96.28.74:58279/kv/get?token=" + token + "&database="+TelecomConfig.DATABASE + "&table="+TelecomConfig.TABLE  + "&key=" + key

    try {
      val result = new JSONObject(Jsoup.connect(url).timeout(5000).execute().body()).get("result").toString

      if (result != "null") {
        value = new JSONObject(result).get("value").toString
        // println("url:" + url + ", value:" + value)
      }
    } catch {
      case e:Exception =>
        UserLogger.exception(e)
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
      case e:Exception => UserLogger.error(e.getMessage)
        null

    }
  }

}