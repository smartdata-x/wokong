package FileUtil

import java.io.{PrintWriter, File}
import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by Administrator on 2016/2/22.
  */
object FileUtil {
  def readFile(array:Array[(Int,String)]): String = {
    //获取当前小时的时间戳 并以此时间戳命名文件夹
    val dataafter = new Date(new Date().getTime)
    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH")
    val strafter = sdf.format(dataafter)
    println(strafter)
    val fileafter = new File("/home/shj/" + strafter)

    //将当前小时的数据写进mapafter集合中 并写进以当前小时时间戳命名的本地文件中
    var arraya = ""
    var i = 1
    for (a <- array) {

      arraya += a._2.toString
      arraya += "\t"
      arraya += i.toString
      i += 1
      arraya += "\n"
      FileUtil.creatFile(arraya,fileafter)
    }
    arraya
  }

  def creatFile(string:String,path:File): Unit ={
      val writer = new PrintWriter(path,"UTF-8")
      writer.write(string)
      writer.flush()
      writer.close()
  }
}
