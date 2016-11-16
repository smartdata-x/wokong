package task

import java.io.File
import java.util.concurrent.Callable

import config.XMLConfig
import ftp.FTPDownload

/**
  * Created by C.J.YOU on 2016/8/12.
  * 根据缺少的文件名来从新获取一次文件（20秒后获取）
  * 多个请求的线程处理类
  */
class RegetTask(fileName:String, fileTime:String) extends Callable[String] {

  override def call(): String = {

    var res = false

    try {

      res = FTPDownload.downloadFile(fileName, fileTime, 1)

    } catch {
      case e: Exception =>
        // ftp backup server
        res  = FTPDownload.downloadFile(fileName, fileTime, 0)

    }

    val fileSize = new File(XMLConfig.ftpConfig.DATA_DIR + "/" + fileTime.substring(0,8) + "/" + fileName).length()

    // 文件下载正常与否处理逻辑
    if(res && fileSize > 0 ) {

     "reget," + fileName + ",Reget successed, file size: " + fileSize + "!!!"

    } else {

      "reget," + fileName + ",Reget failed !!!"

    }


  }

}