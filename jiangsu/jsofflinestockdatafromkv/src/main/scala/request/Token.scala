package request

import java.security.MessageDigest
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import config.TelecomConfig
import org.apache.commons.codec.binary.Hex
import org.json.JSONObject
import org.jsoup.Jsoup

/**
  * Created by C.J.YOU on 2016/8/12.
  * 请求token
  */
object Token {


  def token(): String ={

    var res:String = ""

    val url = "http://180.96.28.74:58279/getToken?apiKey=" + TelecomConfig.API_KEY + "&sign=" + sign(md5Encode(TelecomConfig.PASSWORD), TelecomConfig.USER_NAME + TelecomConfig.API_KEY)
    try {
      val respond = Jsoup.connect(url).timeout(5000).execute()
      res = new JSONObject(respond.body()).get("result").toString

    } catch {
      case e: Exception => println("request token error")
    }


    res

  }


  @throws[Exception]
  private  def sign(secretKey: String, data: String): String = {

    val signingKey: SecretKeySpec = new SecretKeySpec(secretKey.getBytes, TelecomConfig.HMAC_SHA1_ALGORITHM)
    val mac: Mac = Mac.getInstance(TelecomConfig.HMAC_SHA1_ALGORITHM)
    mac.init(signingKey)
    val rawHmac: Array[Byte] = mac.doFinal(data.getBytes)

    Hex.encodeHexString(rawHmac)

  }

  @throws[Exception]
  private  def md5Encode(str: String): String = {

    var md5: MessageDigest = null

    try {
      md5 = MessageDigest.getInstance("MD5")
    }
    catch {
      case e: Exception => {
        return ""
      }
    }

    val byteArray: Array[Byte] = str.getBytes("UTF-8")
    val md5Bytes: Array[Byte] = md5.digest(byteArray)
    val hexValue: StringBuilder = new StringBuilder

    for (md5Byte <- md5Bytes) {

      val `val`: Int = md5Byte.toInt & 0xff

      if (`val` < 16) {
        hexValue.append("0")
      }

      hexValue.append(Integer.toHexString(`val`))

    }

    hexValue.toString

  }

}
