package request

import config.TelecomConfig
import org.json.JSONObject
import org.jsoup.Jsoup
import sun.misc.BASE64Decoder

/**
  * Created by C.J.YOU on 2016/8/12.
  */
object Request {


  def getValue(key: String): String = {

    var value = ""

    val url = "http://180.96.28.74:58279/kv/get?token=" + Token.token() + "&database="+TelecomConfig.DATABASE + "&table="+TelecomConfig.TABLE  + "&key=" + key

    try {
      val result = new JSONObject(Jsoup.connect(url).timeout(3000).execute().body()).get("result").toString

      if (result != "null") {
        value = (new JSONObject(result).get("value").toString)

      }
    } catch {
      case e:Exception => println(e.getMessage)
    }

    // println("value:" + value)

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
