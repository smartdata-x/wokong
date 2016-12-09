package config
import scala.xml.XML
/**
  * Created by C.J.YOU on 2016/8/26.
  * 配置文件的加载
  */

class XMLConfig(xmlFilePath:String) {


  val XML_CONFIG  = XML.loadFile(xmlFilePath)

  val API_KEY = ( XML_CONFIG  \ "KV" \ "APIKEY").text

  val DATABASE = ( XML_CONFIG  \ "KV" \ "DATABASE").text

  val USER_NAME = ( XML_CONFIG  \ "KV" \ "USER").text

  val PASSWORD = ( XML_CONFIG  \ "KV" \ "PASSWORD" ).text

  val TABLE = (XML_CONFIG  \ "KV" \ "TABLE").text

  val LOG_DIR = (XML_CONFIG  \ "FILE" \ "LOG" ).text

  var DATA_DIR = (XML_CONFIG  \ "FILE" \ "DATA").text

  var PROGRESS_DIR =( XML_CONFIG  \ "FILE" \ "PROCESS" ).text

  val LOG_CONFIG = (XML_CONFIG  \ "LOGGER" \ "CONF").text

  val RECEIVER = (XML_CONFIG \ "Message" \ "receiver").text

  val KEY = (XML_CONFIG \ "Message" \ "key").text

  val MESSAGE_CONTEXT = (XML_CONFIG \ "Message" \ "context").text


}


// 伴生对象
object XMLConfig {

  var ftpConfig: XMLConfig =  null

  def apply(xmlFilePath: String) = {

    if(ftpConfig == null)
      ftpConfig = new XMLConfig(xmlFilePath)

    ftpConfig

  }


}
