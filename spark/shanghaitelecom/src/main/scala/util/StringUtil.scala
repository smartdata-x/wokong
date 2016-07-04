package util

import java.io.ByteArrayOutputStream
import java.util.zip.InflaterOutputStream
import config.FileConfig
import log.SUELogger
import org.json.JSONObject
import sun.misc.BASE64Decoder


/**
  * Created by C.J.YOU on 2016/3/16.
  * 字符处理类
  */
object StringUtil extends Serializable{

  /**
    * zlib 解压缩
    * @param compressedStr 压缩的字符串
    * @return 解压缩后的字符串
    */
  private def  zlibUnzip(compressedStr: String ): String = {

    if(compressedStr == null) {
      return null
    }

    val bos = new ByteArrayOutputStream()
    val  zos = new InflaterOutputStream(bos)
    try {
      zos.write(new sun.misc.BASE64Decoder().decodeBuffer(compressedStr))
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      if(zos !=null ){
        zos.close()
      }
      if(bos !=null){
        bos.close()
      }
    }

    new String(bos.toByteArray)

  }

  /**
    * json 格式化
    * @param line 字符串
    * @return 对应的json对象
    */
  private def getJsonObject(line: String): JSONObject = {
    val data = new JSONObject(line)
    data
  }

  /**
    * base64 解码
    * @param base64String 需解码字符串
    * @return 解码后的字符串
    */
  private def decodeBase64(base64String : String): String = {
    val decoded = new BASE64Decoder().decodeBuffer(base64String)
    new String(decoded)
  }

  /**
    * 解析json
    * @param str  源数据
    * @return 解析后的数据
    */
  def parseJsonObject(str:String): String = {

    var finalValue = ""

    try {

      val result = decodeBase64 (str)
      val resultSplit = result.split("_kunyan_")
      val json = getJsonObject(resultSplit(0))
      val keyword = resultSplit(1)
      val id = json.get ("id").toString
      val value = json.get ("value").toString
      val desDe = zlibUnzip(value.replace("-<","\n"))
      val resultJson = desDe.split("\t")
      val ad = resultJson(0)
      val ts = resultJson(1)
      val host = resultJson(2).replace("\n","")
      val url = resultJson(3).replace("\n","")
      val ref = resultJson(4).replace("\n","")
      val ua = resultJson(5).replace("\n","")
      val cookie = resultJson(6).replace("\n","")

      finalValue = ts + "\t" + ad + "\t" + ua + "\t" + host +"\t"+ url + "\t" + ref + "\t" +cookie + "\t" + keyword

    } catch {
      case e:Exception  =>
        SUELogger.error("praseJsonObject ERROR")
        FileUtil.saveErrorData(FileConfig.TOO_BIG_VALUE,str)
    }

  finalValue

  }

  /**
    * 从kafka接受的数据已经是解压缩后的数据
    * @param str 源数据
    * @return 接收后的数据
    */
  def parseNotZlibJsonObject(str:String): String ={

    var finalValue = ""

    try {

      val resultJson = str.split("\t")
      val ad = resultJson(0)
      val ts = resultJson(1)
      val host = resultJson(2).replace("\n","")
      val url = resultJson(3).replace("\n","")
      val ref = resultJson(4).replace("\n","")
      val ua = resultJson(5).replace("\n","")
      val cookie = resultJson(6).replace("\n","")
      val keyword = resultJson(7)

      finalValue = ts + "\t" + ad + "\t" + ua + "\t" + host +"\t"+ url + "\t" + ref + "\t" +cookie + "\t" + keyword

    } catch {
      case e:Exception  =>
        SUELogger.error("praseJsonObject ERROR")
        HDFSUtil.saveToHadoop(FileConfig.TOO_BIG_VALUE,Array(str))
    }
    finalValue
  }
}
