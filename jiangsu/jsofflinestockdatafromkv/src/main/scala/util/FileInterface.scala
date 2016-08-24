package util

/**
  * Created by C.J.YOU on 2016/8/15.
  * 文件保存方法的抽象类
  */
abstract class FileInterface {

  def write(path: String, array:Array[String])

  def writeString(path: String, string: String)

}
