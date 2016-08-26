package ftp

import java.io.{FileOutputStream, IOException}

import config.XMLConfig
import log.UserLogger
import org.apache.commons.io.IOUtils
import org.apache.commons.net.ftp.FTPClient
import util.FileUtil


/**
  * Created by C.J.YOU on 2016/8/26.
  */
object FTPDownload {

  def downloadFile(fileName: String, fileTime: String): Boolean = {


    val ftpClient = new FTPClient()
    ftpClient.connect(XMLConfig.ftpConfig.IP)
    ftpClient.login(XMLConfig.ftpConfig.USER_NAME, XMLConfig.ftpConfig.PASSWORD)

    var fos: FileOutputStream = null
    val remoteFileName = XMLConfig.ftpConfig.REMOTE_DIR + "/" + fileName

    try {


      val dir = XMLConfig.ftpConfig.DATA_DIR + "/" + fileTime.substring(0,8)

      val file = dir + "/" + fileName

      FileUtil.mkDir(dir)

      // println("remoteFileName:" + remoteFileName)

      fos = new FileOutputStream(file)

      ftpClient.setBufferSize(1024)

      val isFileDownload:Boolean = ftpClient.retrieveFile(remoteFileName, fos)

      if(isFileDownload) {
        // ftpClient.deleteFile(remoteFileName)
        true
      } else {
        false
      }


    } catch {

      case e: IOException =>
        // println("FTP连接发生异常:" +e.getMessage )
        UserLogger.error("FTP连接发生异常:" + e.getMessage)

        false

    } finally {

      IOUtils.closeQuietly(fos)

      try {

        ftpClient.disconnect()

      } catch {
        case  e:IOException=>
          // println("关闭FTP连接发生异常:"+ e.getMessage)
          UserLogger.error("关闭FTP连接发生异常！")
      }
    }

  }

}
