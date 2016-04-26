package util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.security.SecureRandom
import java.util.zip.{InflaterOutputStream, GZIPInputStream}
import javax.crypto.{SecretKeyFactory, Cipher}
import javax.crypto.spec.DESKeySpec

import config.FileConfig
import log.SUELogger
import org.json.JSONObject
import sun.misc.BASE64Decoder

/**
  * Created by C.J.YOU on 2016/3/16.
  */
object StringUtil extends Serializable{


  // 解压缩
  private def unCompress(str:String): String = {
    if (str == null || str.length() == 0) {
      return str
    }
    val out = new ByteArrayOutputStream()
    val in = new ByteArrayInputStream(str
      .getBytes("ISO-8859-1"))
    val gunzip = new GZIPInputStream(in)
    val buffer = new Array[Byte](256)
    var n = gunzip.read(buffer)
    while(n >= 0) {
      out.write(buffer, 0, n)
      n = gunzip.read(buffer)
    }
    // toString()使用平台默认编码，也可以显式的指定如toString("GBK")
    out.toString()
  }

  /**
    * <p>Description:使用gzip进行解压缩</p>
    */
  private def  zlibUnzip(compressedStr:String ):String ={

    if(compressedStr == null){
      return null
    }
    val bos = new ByteArrayOutputStream()
    val  zos = new InflaterOutputStream(bos)
    try{
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

  // 电信数据加码后对应的解码 方法
  private def design(str:String): String ={
    val cipher = Cipher.getInstance ("des")
    val keySpec = new DESKeySpec ("kunyandata".getBytes ())
    val keyFactory = SecretKeyFactory.getInstance ("des")
    val secretKey = keyFactory.generateSecret (keySpec)
    cipher.init(Cipher.DECRYPT_MODE, secretKey, new SecureRandom())
    val plainData = cipher.doFinal(new BASE64Decoder().decodeBuffer(str))
    new String(plainData)
  }

  private def getJsonObject(line:String): JSONObject = {
    val data = new JSONObject(line)
    data
  }

  private def decodeBase64(base64String : String): String = {
    val decoded = new BASE64Decoder().decodeBuffer(base64String)
    new String(decoded,"utf-8")
  }

  def parseJsonObject(str:String): String ={

    var result = ""
    try {
      val res = decodeBase64 (str)
      // println(res)
      val json = getJsonObject (res)
      // println(json)
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
      val keyword = resultJson(7)
      result = ts + "\t" + ad + "\t" + ua + "\t" + host +"\t"+ url + "\t" + ref + "\t" +cookie + "\t" + keyword
    } catch {
      case e:Exception  =>
        SUELogger.error("praseJsonObject ERROR")
        FileUtil.saveErrorData(FileConfig.TOO_BIG_VALUE,str)
    }
    result
  }
}
