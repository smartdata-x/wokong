package com.kunyan.data.util

import java.io._

import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/5/27.
  * HDFS操作的工具类
  */
object FileUtil {

  /** 创建目录 */
  def mkDir(name: String): Boolean = {

    val dir = new File(name)
    dir.mkdir
  }

  def readFile(path: String): ListBuffer[String] = {

    var lines = new ListBuffer[String]()

    val br = new BufferedReader(new FileReader(path))

    try {
      var line = br.readLine()

      while (line != null) {
        lines += line
        line = br.readLine()
      }
      lines
    } finally {
      br.close()
    }
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

  /**
    * override the old one
 *
    * @author yangshuai
    */
  def createFile(path: String, lines: Seq[String], num: Int): Unit = {

    val writer = new PrintWriter(path, "UTF-8")
    var count = num
    if(count == 0){
      writer.println("no user exist")
      writer.close()
      System.exit(-1)
    }
    for (line <- lines) {
      if (count > 0) {
        writer.println(line)
        count -= 1
      }
    }
    writer.close()
  }

  private def isExist(path:String): Boolean ={
    val file = new File(path)
    file.exists()
  }

  private def createFile(path:String): Unit ={
    val file = new File(path)
    if(!isExist(path)){
      file.createNewFile()
    }
  }

  def writeToFile(path: String, array:Array[String]): Unit = {

    createFile(path)
    val out = new FileOutputStream(new File(path),true)
    val writer = new PrintWriter(out, false)
    for (line <- array){
      writer.append(line + "\n")
    }
    writer.flush()
    writer.close()
  }

  def writeUserFollowToFile(path: String, array:Array[(String,String)],typeC: Int): Unit = {

    createFile(path)
    val out = new FileOutputStream(new File(path),true)
    val writer = new PrintWriter(out, false)
    for (line <- array){
      if(typeC == 0)
        writer.append(line._2 +"\t"+ line._1 + "\n")
      else
        writer.append(line._2 + "\n")
    }
    writer.flush()
    writer.close()
  }

  def createFile(path: String, lines:String): Unit = {
    val writer = new PrintWriter(path, "UTF-8")
    writer.println(lines)
    writer.close()
  }

  def readUserFile(dir: File): Iterator[File] = {
    val d = dir.listFiles.filter(_.isDirectory)
    val f = dir.listFiles.toIterator
    f ++ d.toIterator.flatMap(readUserFile)
  }
}
