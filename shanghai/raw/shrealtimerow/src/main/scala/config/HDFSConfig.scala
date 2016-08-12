package config

/**
  * Created by C.J.YOU on 2016/6/30.
  * HDFS目录配置
  */
object HDFSConfig {

  var HDFS_NAMENODE = ""
  var HDFS_ROOT_DIR = ""
  var HDFS_SEARCH_ENGINE_DATA = ""
  var HDFS_TOO_BIG_VALUE = ""


  def nameNode(path: String): Unit = {
    HDFS_NAMENODE = path
  }

  /**
    * 主要数据data路径
    * @param dir 目录
    */
  def  rootDir(dir:String): Unit = {
    HDFS_ROOT_DIR = HDFS_NAMENODE + dir
  }

  /**
    * 搜索数据目录
    * @param dir 目录
    */
  def searchEngineDir(dir:String):Unit = {
    HDFS_SEARCH_ENGINE_DATA = HDFS_NAMENODE + dir
  }

  /**
    * 记录解析出错数据目录
    * @param dir 目录
    */
  def errorDataDir(dir:String):Unit = {
    HDFS_TOO_BIG_VALUE = HDFS_NAMENODE + dir
  }

}
