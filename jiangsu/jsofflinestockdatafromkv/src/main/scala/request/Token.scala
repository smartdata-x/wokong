package request

import java.security.MessageDigest
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import config.TelecomConfig
import log.UserLogger
import org.apache.commons.codec.binary.Hex
import org.json.JSONObject
import org.jsoup.Jsoup

/**
  * Created by C.J.YOU on 2016/8/12.
  * 请求token
  */
object Token {


  /**
    * 获取token，请求value时需要token
    * @return
    */
  def token(): String = {

    var res:String = ""

    val url = "http://180.96.28.74:58279/getToken?apiKey=" + TelecomConfig.API_KEY + "&sign=" + sign(md5Encode(TelecomConfig.PASSWORD), TelecomConfig.USER_NAME + TelecomConfig.API_KEY)

    try {

      val respond = Jsoup.connect(url).timeout(5000).execute()
      res = new JSONObject(respond.body()).get("result").toString

    } catch {
      case e: Exception =>
        UserLogger.error("request token error")
    }

    res

  }


  /**
    * 接口秘钥算法实现
    * @param secretKey 秘钥
    * @param data 需要加密的数据
    * @return  加密后结果数据
    */
  private  def sign(secretKey: String, data: String) = {

    val signingKey: SecretKeySpec = new SecretKeySpec(secretKey.getBytes, TelecomConfig.HMAC_SHA1_ALGORITHM)
    val mac: Mac = Mac.getInstance(TelecomConfig.HMAC_SHA1_ALGORITHM)
    mac.init(signingKey)
    val rawHmac: Array[Byte] = mac.doFinal(data.getBytes)

    Hex.encodeHexString(rawHmac)

  }

  /**
    * md5处理密码
    * @param str 密码
    * @return MD5处理后结果
    */
  private  def md5Encode(str: String): String = {

    var md5: MessageDigest = null

    try {
      md5 = MessageDigest.getInstance("MD5")
    }
    catch {
      case e: Exception =>
        return ""
    }

    val byteArray: Array[Byte] = str.getBytes("UTF-8")
    val md5Bytes: Array[Byte] = md5.digest(byteArray)
    val hexValue: StringBuilder = new StringBuilder

    for (md5Byte <- md5Bytes) {

      val res: Int = md5Byte.toInt & 0xff

      if (res < 16) {
        hexValue.append("0")
      }

      hexValue.append(Integer.toHexString(res))

    }

    hexValue.toString

  }

}