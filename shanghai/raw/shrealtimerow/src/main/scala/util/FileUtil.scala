package util

import java.io._

/**
  * Created by C.J.YOU on 2016/1/14.
  * HDFS操作的工具类
  */
object FileUtil {

  private def isExist(path:String): Boolean ={
    val file = new File(path)
    file.exists()
  }
  /** 创建目录 */
  private def mkDir(name: String): Unit = {
    val dir = new File(name)
    if(!isExist(name)){
      dir.mkdir
    }
  }
  private def createFile(path:String): Unit ={
    val file = new File(path)
    if(!isExist(path)){
      file.createNewFile()
    }
  }

  private def writeToFile(path: String, array:Array[String]): Unit = {
    val out = new FileOutputStream(new File(path),true)
    val writer = new PrintWriter(out, false)
    for (arr <- array){
      writer.append(arr + "\n")
    }
    writer.flush()
    writer.close()
  }

  private def writeStringToFile(path: String, str:String): Unit = {
    val out = new FileOutputStream(new File(path),true)
    val writer = new PrintWriter(out, false)
    writer.append(str + "\n")
    writer.flush()
    writer.close()
  }

  /**
    * log file
    * @param rootDir 保存目录
    * @param data log 数据
    */
  def saveLog(rootDir: String, data: Array[String]): Unit = {
    val logDir = rootDir + "/" + TimeUtil.getDay
    val logFile = rootDir + "/" + TimeUtil.getDay +"/"+TimeUtil.getCurrentHour
    FileUtil.mkDir(logDir)
    FileUtil.createFile(logFile)
    FileUtil.writeToFile(logFile,data)
  }

  def saveData(rootDir:String,data:Array[String]):Unit ={
    val searchEngineDir = rootDir + "/" + TimeUtil.getDay
    val searchEngineFile = rootDir + "/" + TimeUtil.getDay +"/"+TimeUtil.getCurrentHour
    FileUtil.mkDir(searchEngineDir)
    FileUtil.createFile(searchEngineFile)
    FileUtil.writeToFile(searchEngineFile,data)
  }

  def saveErrorData(rootDir:String,data:String): Unit ={
    val dir = rootDir + "/" + TimeUtil.getDay
    val file = rootDir + "/" + TimeUtil.getDay +"/"+TimeUtil.getCurrentHour+"_tbv"
    FileUtil.mkDir(dir)
    FileUtil.createFile(file)
    FileUtil.writeStringToFile(file,data)
  }
}
