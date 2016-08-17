package util

import java.io._

/**
  * Created by C.J.YOU on 2016/1/14.
  * HDFS操作的工具类
  */
object FileUtil {

  /** 创建目录 */
  def mkDir(name: String): Boolean = {

    val dir = new File(name)
    dir.mkdir
  }

  /**
    * override the old one
    * @author yangshuai
    */
  def createFile(path: String, lines: Seq[String]): Unit = {

    val writer = new PrintWriter(path, "UTF-8")
    for (line <- lines) {
      writer.println(line)
    }
    writer.close()
  }

  def readUserFile(dir: File): Iterator[File] = {

    val d = dir.listFiles.filter(_.isDirectory)
    val f = dir.listFiles.toIterator
    f ++ d.toIterator.flatMap(readUserFile)

  }

  /**
    * 判断文件是否存在
    * @param path 文件路径
    * @return 存在返回true，否则返回false
    */
  private def isExist(path:String): Boolean = {

    val file = new File(path)
    file.exists()

  }

  /**
    * 创建文件
    * @param path 文件路径
    */
  private def createFile(path:String): Unit = {

    val file = new File(path)

    if(!isExist(path)){
      file.createNewFile()
    }

  }

  /**
    * 写stock热度数据到本地CSV中，方便需要时导入到数据库中
    * @param path 存储文件路径
    * @param array 存储的数据形式
    */
  def writeToCSV(path: String, array:Array[(String,String,String)]): Unit = {

    createFile(path)
    val out = new FileOutputStream(new File(path),true)
    val writer = new PrintWriter(out, false)
    writer.append("\"v_stock\",\"v_hour\",\"i_frequency\"" + "\n")

    for (line <- array){
      writer.append("\""+line._1+"\",\""+line._2+"\",\""+line._3+"\"\n")
    }

    writer.flush()
    writer.close()

  }

  /**
    * stock 热度数据不同匹配规则有效数据的统计，存储为CSV。
    * @param path 存储的文件路径
    * @param array 存储的数据格式
    */
  def writeRuleCountToCSV(path: String, array:Array[(String,String)]): Unit = {

    createFile(path)
    val out = new FileOutputStream(new File(path),true)
    val writer = new PrintWriter(out, false)
    writer.append("\"v_rule\",\"i_frequency\"" + "\n")

    for (line <- array){
      writer.append("\""+line._1+"\",\""+line._2+"\"\n")
    }

    writer.flush()
    writer.close()

  }

}
