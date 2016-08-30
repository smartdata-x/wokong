package config

import scala.xml.XML

/**
  * Created by C.J.YOU on 2016/8/26.
  * 配置文件的加载
  */

class XMLConfig(xmlFilePath:String) {


  val xmlConfig  = XML.loadFile(xmlFilePath)

  val IP = ( xmlConfig  \ "FTP" \ "IP").text

  val USER_NAME = ( xmlConfig  \ "FTP" \ "USER").text

  val PASSWORD = ( xmlConfig  \ "FTP" \ "PASSWORD" ).text

  val REMOTE_DIR = (xmlConfig  \ "FTP" \ "ROOTDIR").text

  val FILE_PREFIX_NAME = (xmlConfig  \ "FTP" \ "PREFFIX").text

  val FILE_SUFFIX_NAME = (xmlConfig  \ "FTP" \ ".DAT" ).text

  val LOG_DIR = (xmlConfig  \ "FILE" \ "LOG" ).text

  var DATA_DIR = (xmlConfig  \ "FILE" \ "DATA").text

  var PROGRESS_DIR =( xmlConfig  \ "FILE" \ "PROCESS" ).text

  val LOG_CONFIG = (xmlConfig  \ "LOGGER" \ "CONF").text

  val RECEIVER = (xmlConfig \ "Message" \ "receiver").text

  val KEY = (xmlConfig \ "Message" \ "key").text

  val MESSAGE_CONTEXT = (xmlConfig \ "Message" \ "context").text


}


// 伴生对象
object XMLConfig {

  var ftpConfig: XMLConfig =  null

  def apply(xmlFilePath: String):Unit = {

    if(ftpConfig == null)
      ftpConfig = new XMLConfig(xmlFilePath)

  }


}
