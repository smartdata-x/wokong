package task

import java.io.File
import java.util.concurrent.Callable

import config.XMLConfig
import ftp.FTPDownload

/**
  * Created by C.J.YOU on 2016/8/12.
  * key 为分钟级别细分两部分（0 与 5）: second 用来对秒取整
  * 多个请求的线程处理类
  */
class Task (key: String, second: Int, last:Int, taskId: Int) extends Callable[String] {

  override def call(): String = {

    val sec = if(second == 0)  "0" + last else second * 10 + last

    val fileTime = key + sec

    val fileName = XMLConfig.ftpConfig.FILE_PREFIX_NAME +  fileTime + XMLConfig.ftpConfig.FILE_SUFFIX_NAME

    var res = false

    try {

      res = FTPDownload.downloadFile(fileName, fileTime, 1)

    } catch {
      case e: Exception =>
        // ftp backup server
        res  = FTPDownload.downloadFile(fileName, fileTime, 0)

    }

    val fileSize = new File(XMLConfig.ftpConfig.DATA_DIR + "/" + fileTime.substring(0,8) + "/" + fileName).length() / 1024

    // 文件下载正常与否处理逻辑
    if(res && fileSize > 0 ) {

      taskId + "," + fileName + ",successed, file size: " + fileSize + "!!!"

    } else {

      taskId + "," + fileName + ",failed !!!"

    }


  }

}