package com.kunyan.companyinfo.parser.json

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

/**
  * Created by niujiaojiao on 2016/7/24.
  * 股本结构模块
  */
object StructJson {

  /**
    * 股本结构解析hbase表获取的字符串入口
    *
    * @param json hbase 表获取的字符串
    * @return 将要插入数据库的数据集合：分为三个表
    */
  def parse(json: String): (ListBuffer[ListBuffer[String]], ListBuffer[ListBuffer[String]]
    , ListBuffer[ListBuffer[String]]) = {

    var limitList = new ListBuffer[ListBuffer[String]]()
    var distributionList = new ListBuffer[ListBuffer[String]]()
    var historyList = new ListBuffer[ListBuffer[String]]()
    val map = JSON.parseFull(json)

    if (map.isEmpty) {

      println("\"JSON parse value is empty,please have a check!\"")

    } else {

      try{

        val shareMap = map.asInstanceOf[Map[String, AnyVal]].getOrElse("股本结构", "").asInstanceOf[Map[String, AnyVal]]
        limitList = CommonJson.parse(shareMap.getOrElse("1", "").asInstanceOf[Map[String, AnyVal]], 0)
        distributionList = CommonJson.parse(shareMap.getOrElse("2", "").asInstanceOf[Map[String, AnyVal]], 0)

        val historyMap = map.asInstanceOf[Map[String, AnyVal]].getOrElse("历年股本变动", "").asInstanceOf[Map[String, AnyVal]]
        historyList = specialSituation(historyMap)

      }catch {
        case e: Exception =>
          e.printStackTrace()
      }

    }

    (limitList, distributionList, historyList)
  }



  /**
    * 针对股本结构；历年股本变动这一模块的解析，hbase 表数据存储，取得的是表格一行，数据库设计的
    * 是需要该表格一列的数据，需要进行转换
    *
    * @param mapJson map 键值对集合
    * @return 所需要的表格中的每列数据的集合
    */
  def specialSituation(mapJson: Map[String, AnyVal]): ListBuffer[ListBuffer[String]] = {

    var outList = new ListBuffer[ListBuffer[String]]()

    val ids = mapJson.keys

    ids.foreach {

      index => {

        //最里层的ListBuffer[] 得到一行的数据
        var inList = new ListBuffer[String]()
        val values = mapJson.getOrElse(index, "")

        if (values.toString.nonEmpty) {

          val sub = values.asInstanceOf[Map[String, AnyVal]]
          val keys = sub.keys

          keys.foreach {

            y => {

              if (y.toString != "") {

                var result = sub.getOrElse(y, "")

                //也要加上第一行的key值： 针对历年股本变动
                if (result.toString.startsWith("--"))
                  result = ""

                if (result.toString.contains(","))
                  result = result.toString.replace(",", "")

                //加上第一行第一列的元素Key
                inList += index.toString
                inList += y.toString
                inList += result.toString

              }

            }

          }

        }

        outList += inList

      }

    }

    var tranList = new ListBuffer[ListBuffer[String]]()

    if (outList.toString().nonEmpty) {

      if (outList.nonEmpty) {

        if (outList.head.nonEmpty) {

          val size = outList.head.size / 3
          var date = ""
          var rowkey = ""

          for (i <- 0 until size) {

            var tranInList = new ListBuffer[String]()

            for (j <- outList.indices) {

              date = outList(j)(i * 3 + 1)
              rowkey = outList(j).head
              val value = outList(j)(i * 3 + 2)
              tranInList += rowkey
              tranInList += value

            }

            tranInList += date
            tranList += tranInList

          }

        }

      }

    }

    tranList
  }

}
