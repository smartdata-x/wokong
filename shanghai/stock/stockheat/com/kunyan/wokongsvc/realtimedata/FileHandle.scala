/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/FileHandle.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-07-18 19:38
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import java.io.BufferedReader
import java.io.File 
import java.io.FileWriter
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import java.io.FileInputStream
import java.io.InputStreamReader

/**
  * Created by wukun on 2016/07/18
  * 操作文件句柄
  */
class FileHandle(val path: String) {

  lazy val file: File = createFile

  def createFile(): File = {

    val sourceFile = Try(new File(path)) match {

      case Success(file) => file
      case Failure(e) => System.exit(-1)

    }

    sourceFile.asInstanceOf[File]
  }

  /**
    * 获取写句柄
    * @author wukun
    */
  def createWriter(): FileWriter = {

    val writer = Try(new FileWriter(path, false)) match {
      case Success(writer) => writer
      case Failure(e) => System.exit(-1)
    }

    writer.asInstanceOf[FileWriter]
  }

  /**
    * 获取读句柄
    * @author wukun
    */
  def createReader(): BufferedReader = {

    val read = new InputStreamReader(new FileInputStream(file.asInstanceOf[File]))
    val bufferedReader = new BufferedReader(read)

    bufferedReader
  }
}

/**
  * Created by wukun on 2016/7/18
  * 文件操作句柄伴生对象
  */
object FileHandle {

  def apply(path: String): FileHandle = {
    new FileHandle(path)
  }

}





