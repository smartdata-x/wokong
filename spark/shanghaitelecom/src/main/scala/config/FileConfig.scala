package config

/**
  * Created by C.J.YOU on 2016/1/18.
  * 本地目录配置文件
  */
object FileConfig {

  var ROOT_DIR = "/home/telecom/data"
  var SEARCH_ENGINE_DATA ="/home/telecom/SearchEngineData"
  var TOO_BIG_VALUE="/home/telecom/ErrorData"
  val test_dir = "F:\\datatest\\telecom"

  /**
    * 主要数据data路径
    * @param dir 目录
    */
  def  rootDir(dir:String): Unit = {
    ROOT_DIR = dir
  }

  /**
    * 搜索数据目录
    * @param dir 目录
    */
  def searchEngineDir(dir:String):Unit ={
    SEARCH_ENGINE_DATA = dir
  }

  /**
    * 记录解析出错数据目录
    * @param dir 目录
    */
  def errorDataDir(dir:String):Unit = {
    TOO_BIG_VALUE = dir
  }
}