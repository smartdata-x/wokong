package config

import scala.xml.XML

/**
  * Created by C.J.YOU on 2016/8/2.
  */
object XMLConfig {


  var path: String = null

  def setPath(string: String) = {
    path = string
  }

  val confFile = XML.loadFile(path)

}
