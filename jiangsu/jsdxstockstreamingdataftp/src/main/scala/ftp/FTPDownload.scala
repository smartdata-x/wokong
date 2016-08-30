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


  /**
    * ftp 下载
    * @param fileName 文件名
    * @param fileTime 文件中的时间信息
    * @return 下载成功与否
    */
  def downloadFile(fileName: String, fileTime: String): Boolean = {


    // ftp 连接
    val ftpClient = new FTPClient()
    ftpClient.connect(XMLConfig.ftpConfig.IP)
    ftpClient.login(XMLConfig.ftpConfig.USER_NAME, XMLConfig.ftpConfig.PASSWORD)

    var fos: FileOutputStream = null
    val remoteFileName = XMLConfig.ftpConfig.REMOTE_DIR + "/" + fileName

    try {


      val dir = XMLConfig.ftpConfig.DATA_DIR + "/" + fileTime.substring(0,8)

      val file = dir + "/" + fileName

      FileUtil.mkDir(dir)

      fos = new FileOutputStream(file)

      ftpClient.setBufferSize(1024)

      val isFileDownload:Boolean = ftpClient.retrieveFile(remoteFileName, fos)

      if(isFileDownload) {

        // 删除远程ftp的文件
        ftpClient.deleteFile(remoteFileName)

        true

      } else  false


    } catch {

      case e: IOException =>
        UserLogger.error("FTP连接发生异常:" + e.getMessage)
        false

    } finally {

      IOUtils.closeQuietly(fos)

      try {

        ftpClient.disconnect()

      } catch {
        case  e:IOException=>
          UserLogger.error("关闭FTP连接发生异常！")
      }
    }

  }

}
