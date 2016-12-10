package com.kunyan.util

import java.io._


/**
  * Created by C.J.YOU on 2016/1/14.
  * File 操作的工具类
  */
object FileUtil {

  private def isExist(path:String): Boolean ={
    val file = new File(path)
    file.exists()
  }

  /** 创建目录 */
  def mkDir(name: String): Unit = {
    val dir = new File(name)
    if(!isExist(name)){
      dir.mkdir
    }
  }

  /**
    * create file
    *
    * @param path file path
    */
  def createFile(path:String): Unit ={
    val file = new File(path)
    if(!isExist(path)){
      file.createNewFile()
    }
  }

  /**
    * write to file
    *
    * @param path file path
    * @param array data
    * @param isAppend 文件追加与否  if true, then bytes will be written
    *                   to the end of the file rather than the beginning
    */
  def filterStockCodeWriter(path: String, array:Array[(Array[String],String)],isAppend:Boolean=false): Unit = {

    val file = new File(path)
    if(file.exists()) {
      file.delete()
    }
    val out = new FileOutputStream(new File(path),isAppend)
    val writer = new PrintWriter(out, false)
    for (arr <- array){
      writer.append(arr._1.mkString("\t"))
      writer.append("\t" + arr._2 + "\n")
    }
    writer.flush()
    writer.close()
  }

  def normalWriter(path: String, array:Array[String], isAppend:Boolean = false): Unit = {

    val out = new FileOutputStream(new File(path),isAppend)
    val writer = new PrintWriter(out, false)
    for (item <- array) {
      writer.append(item + "\n")
    }
    writer.flush()
    writer.close()
  }
}
