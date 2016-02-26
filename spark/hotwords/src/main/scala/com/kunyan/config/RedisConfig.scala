package com.kunyan.config

/**
  * Created by yangshuai on 2016/1/27.
  */
object RedisConfig {

  var _ip = ""
  var _port = 0
  var _auth = ""

  def init(ip: String, port: Int, auth: String): Unit = {
    _ip = ip
    _port = port
    _auth = auth
  }
  
  def ip: String = _ip

  def port: Int = _port

  def auth: String = _auth
}
