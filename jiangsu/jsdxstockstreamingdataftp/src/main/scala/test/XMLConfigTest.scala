package test

import java.io.File

import config.XMLConfig

/**
  * Created by C.J.YOU on 2016/8/26.
  */
object XMLConfigTest {

  def main(args: Array[String]) {

    val  xml = XMLConfig.apply("E:\\jsdxftpdown.xml")

    println("ip:" + XMLConfig.ftpConfig.IP)

    println(new File("F:\\jsdx\\streaming\\kunyanstream20160826112105.DAT").length() / 1024)

  }

}
