package util

import java.io._

/**
  * Created by C.J.YOU on 2016/8/13.
  * FileSystem 操作的工具类
  */
object FileUtil extends FileInterface {

  /**
    * 判断文件是否村子啊
    * @param path 路径
    * @return 存在true，false：不存在
    */
  private def isExist(path:String): Boolean = {

    val file = new File(path)
    file.exists()

  }

  /**
    * 创建目录
    * @param name 指定目录名
    */
  def mkDir(name: String): Unit = {

    val dir = new File(name)

    if(!isExist(name)) {
      dir.mkdir
    }

  }

  /**
    * 创建文件
    * @param path 文件路径
    */
  private def createFile(path:String): Unit = {

    val file = new File(path)

    if(!isExist(path)) {
      file.createNewFile()
    }

  }

  /**
    * 写入String到文件中
    * @param path 文件路径
    * @param data String
    */
  override def writeString(path: String, data:String): Unit = {

    write(path, Array(data))

  }

  /**
    * 写入String数组到文件中
    * @param path 文件路径
    * @param array Array[String]
    */
  override def write(path: String, array: Array[String]): Unit = {

      createFile(path)
      val out = new FileOutputStream(new File(path),true)
      val writer = new PrintWriter(out, false)

      for (arr <- array) {
        writer.append(arr + "\n")
      }

      writer.flush()
      writer.close()
  }

}