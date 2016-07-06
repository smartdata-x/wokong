package util

import java.io.{BufferedInputStream, ByteArrayInputStream}
import java.net.URI

import config.HDFSConfig
import log.SUELogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils

/**
  * Created by C.J.YOU on 2016/6/30.
  */
object HDFSUtil {

  val conf = new Configuration()
  conf.setBoolean("dfs.support.append",true)

  val fs = FileSystem.get(new URI(HDFSConfig.HDFS_NAMENODE), conf)

  /**
    * 在HDFS上创建文件
    * @param day 文件
    */
  def createFile(day: String): Path = {

    val file = new Path(day + "/" + TimeUtil.getCurrentHour)

    if(!fs.exists(file)) {
      fs.create(file, false)
    }

    file

  }

  /**
    * 在hdfs上创建目录
    * @param dir 目录路径
    */
  def mkDir(dir: Path): Unit = {

    if(!fs.exists(dir)) {
      fs.mkdirs(dir)
    }

  }

  /**
    * 将数据写入HDFS中
    * @param path 写入数据路径
    * @param data 写入的数据
    */
  def writeToFile(path: Path , data: Array[String]): Unit = {

    val sb = new StringBuilder()

    for (line <- data) {
      sb.append(line + "\n")
    }

    var in: BufferedInputStream = null
    var out: FSDataOutputStream = null

    try {

      out = fs.append(path)
      in = new BufferedInputStream(new ByteArrayInputStream(sb.toString().getBytes))
      IOUtils.copyBytes(in, out, 4096)

    } catch {
      case e: Exception =>  SUELogger.exception(e)
    } finally {
      sb.clear()
      in.close()
      out.close()
      fs.close()
    }

  }

  /**
    * 写入data，search数据
    * @param filePath 指定目录
    * @param data 数据
    */
  def saveData(filePath: Path, data: Array[String]): Unit = {

    writeToFile(filePath, data)

  }

  /**
    * 写入hadoop fs
    * @param dir  指定目录
    * @param data 数据
    */
  def saveToHadoopFileSystem(dir: String, data:Array[String]): Unit = {

    mkDir(new Path(dir))
    val day = new Path(dir + "/" + TimeUtil.getDay)
    mkDir(day)
    val file = HDFSUtil.createFile(dir + "/" + TimeUtil.getDay)
    saveData(file, data)

  }

}
