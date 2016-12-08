package com.kunyan.config

import scala.xml.XML

/**
  * Created by C.J.YOU on 2016/8/26.
  * 配置文件的加载
  */

class SXMLConfig(xmlFilePath:String) {


  private val xmlConfig = loadXml


  def loadXml = XML.loadFile(xmlFilePath)

  def getElem(elemName:String) = (xmlConfig \ elemName).text

  def getElem(firstName:String, secondName:String) = (xmlConfig \ firstName \ secondName).text

  override def toString = this.xmlFilePath

  val tableUp = getElem("table", "tableUp")

  val tableSk = getElem("table", "tableSk")

  val expireDay = getElem("table", "expireDay")

  val sendTopic = getElem("kafka", "sendTopic")

  val topics = getElem("kafka", "topics")

  val zkQuorum = getElem("kafka", "zkQuorum")

  val groupId = getElem("kafka", "groupId")

  val indexUrl = getElem("url", "location")


}


// 伴生对象
object SXMLConfig {

  private  var  xmlHandler: SXMLConfig =  null

  def apply(xmlFilePath: String) = {

    if( xmlHandler == null)
       xmlHandler = new SXMLConfig(xmlFilePath)

    xmlHandler

  }

  def getInstance = xmlHandler

}
