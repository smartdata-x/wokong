package config

import scala.xml.XML

/**
  * Created by C.J.YOU on 2016/8/2.
  */
class XMLConfig (path: String) {

  val confFile = XML.loadFile(path)
  val RECEIVER = (confFile \ "Message" \ "receiver").text
  val KEY = (confFile \ "Message" \ "key").text
  val MESSAGE_CONTEXT = (confFile \ "Message" \ "context").text

}

object  XMLConfig {

  def apply(path: String):  XMLConfig = new XMLConfig(path)

}
