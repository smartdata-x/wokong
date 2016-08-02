package message

import java.net.URLEncoder

import scala.io.Source

/**
  * Created by C.J.YOU on 2016/8/2.
  */
object TextSender {

  /**
    * 发送短信
    * key向组长索取
    * @param key 短信服务authKey
    * @param content 短信内容
    * @param phone 手机号码(超过一个用英文逗号分隔)
    *
    */
  def send(key: String, content: String, phone: String): Boolean = {
    val urlEncodeContent = URLEncoder.encode(content, "UTF-8")
    val url = String.format("http://smsapi.c123.cn/OpenPlatform/OpenApi?action=sendOnce&ac=1001@501318590001&authkey=%s&cgid=52&csid=50131859&c=%s&m=%s", key, urlEncodeContent, phone)
    val result = Source.fromURL(url).mkString
    result.contains("result=\"1\"")
  }

}
