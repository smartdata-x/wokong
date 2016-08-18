package util

import java.io._

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/8/13.
  * FileSystem 操作的工具类
  */
object FileUtil extends  FileInterface{

  private def isExist(path:String): Boolean ={
    val file = new File(path)
    file.exists()
  }

  /**
    * 创建目录
    *
    * @param name 指定目录名
    */
  def mkDir(name: String): Unit = {
    val dir = new File(name)
    if(!isExist(name)){
      dir.mkdir
    }
  }

  private def createFile(path:String): Unit = {
    val file = new File(path)
    if(!isExist(path)){
      file.createNewFile()
    }
  }

  def writeToFile(path: String, array:ListBuffer[String]): Unit = {

    createFile(path)
    val out = new FileOutputStream(new File(path),true)
    val writer = new PrintWriter(out, false)

    for (arr <- array){
      writer.append(arr + "\n")
    }
    writer.flush()
    writer.close()
  }

  def writeString(path: String, array:String): Unit = {

    createFile(path)
    val out = new FileOutputStream(new File(path),true)
    val writer = new PrintWriter(out, false)
    writer.append(array + "\n")

    writer.flush()
    writer.close()

  }

  override def write(path: String, array: Array[(String,Int)]): Unit = {

    createFile(path)
    val out = new FileOutputStream(new File(path),true)
    val writer = new PrintWriter(out, false)

    for (arr <- array){
      writer.append(arr + "\n")
    }

    writer.flush()
    writer.close()
  }
}