package com.kunyan.config

import scala.xml.XML

/**
  * Created by C.J.YOU on 2016/8/26.
  * 配置文件的加载
  */

class XMLConfig(xmlFilePath:String) {


  val XML_CONFIG  = XML.loadFile(xmlFilePath)

  val tableUp = ( XML_CONFIG  \ "table" \ "tableUp").text

  val tableSk = ( XML_CONFIG  \ "table" \ "tableSk").text

  val expireDay = ( XML_CONFIG  \ "table" \ "expireDay").text

  val sendTopic = ( XML_CONFIG  \ "kafka" \ "sendTopic" ).text

  val topics = (XML_CONFIG  \ "kafka" \ "topics").text

  val zkQuorum = (XML_CONFIG  \ "kafka" \ "zkQuorum").text

  val groupId = (XML_CONFIG  \ "kafka" \ "groupId").text

  val indexUrl = (XML_CONFIG  \ "url" \ "location").text


}


// 伴生对象
object XMLConfig {

  var ftpConfig: XMLConfig =  null

  def apply(xmlFilePath: String):Unit = {

    if(ftpConfig == null)
      ftpConfig = new XMLConfig(xmlFilePath)

  }


}
