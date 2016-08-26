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

    // println("fileName:" + fileName)

    val res = FTPDownload.downloadFile(fileName, fileTime)

    if(res) {
      taskId + "," + fileName + ",successed, file size: " + new File(XMLConfig.ftpConfig.DATA_DIR + "/" + fileTime.substring(0,8) +"/" + fileName).length() / 1024 + "!!!"
    } else {
      taskId + "," + fileName + ",failed !!!"
    }


  }

}