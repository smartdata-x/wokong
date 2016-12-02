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
import java.io.FileInputStream
import java.io.FileReader
import java.io.FileWriter
import java.io.InputStreamReader
import logger.HeatLogger

import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Created by wukun on 2016/07/18
  * 操作文件句柄
  */
class FileHandle(val path: String) {

  lazy val file: File = initFile()

  def initFile(): File = {

    val sourceFile = Try(new File(path)) match {

      case Success(source) =>
        source
      case Failure(e) =>
        HeatLogger.exception(e)
        System.exit(-1)

    }

    sourceFile.asInstanceOf[File]
  }

  /**
    * 获取写句柄
    *
    * @author wukun
    */
  def initWriter(): FileWriter = {

    val writer = Try(new FileWriter(path, false)) match {
      case Success(writer) => writer
      case Failure(e) =>
        HeatLogger.exception(e)
        System.exit(-1)
    }

    writer.asInstanceOf[FileWriter]
  }

  /**
    * 获取读句柄
    *
    * @author wukun
    */
  def initReader(): FileReader = {

    val reader = Try(new FileReader(path)) match {
      case Success(reader) => reader
      case Failure(e) =>
        HeatLogger.exception(e)
        System.exit(-1)
    }

    reader.asInstanceOf[FileReader]
  }

  /**
    * 获取读缓存句柄
    *
    * @author wukun
    */
  def initBuff(): BufferedReader = {

    val bufferedReader = new BufferedReader(new FileReader(path))

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

