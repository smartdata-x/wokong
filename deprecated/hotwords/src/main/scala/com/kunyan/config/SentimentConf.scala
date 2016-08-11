package com.kunyan.config

import org.json.JSONObject

import scala.io.Source


/**
  * Created by QQ on 4/26/16.
  */
class SentimentConf{

  private var config = new JSONObject()

  def initConfig(path: String): Unit = {
    val jsObj = Source.fromFile(path).getLines().mkString("")
    config = new JSONObject(jsObj)
  }

  def getValue(keyMom: String, keySon: String): String = {
    config.getJSONObject(keyMom).getString(keySon)
  }

}