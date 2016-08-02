package com.kunyan.companyinfo.parser

import com.kunyan.companyinfo.parser.html.{HolderHtml, StructHtml, LeaderHtml, InfoHtml}
import scala.io.Source

/**
  * Created by niujiaojiao on 2016/7/29.
  */
object StoreData {

  /**
    * 数据写入hbase表中
    *
    * @param fileName 文件名
    */
  def writeToTable(fileName: String): Unit = {

    val lines = Source.fromFile(ClassLoader.getSystemResource(fileName).toString.split("file:/")(1)).getLines()

    lines.foreach{

      x=>{

        val firstJson = InfoHtml.parseCpnyInstructure(x)
        val secondJson = LeaderHtml.parseCpnyExecutives(x)
        val thirdJson =  StructHtml.parseCapitalStructure(x)
        val fourthJson = HolderHtml.parseStockHolders(x)

      }

    }

  }

}
