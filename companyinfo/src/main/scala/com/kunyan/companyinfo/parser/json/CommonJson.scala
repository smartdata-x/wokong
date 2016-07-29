package com.kunyan.companyinfo.parser.json

import scala.collection.mutable.ListBuffer

/**
  * Created by niujiaojiao on 2016/7/28.
  */
object CommonJson {

  /**
    *
    * @param mapJson    Map 集合键值对
    * @param identifier 如若数字中存在逗号，需要把逗号去除，得到整个的数字字符串
    * @return 所需要的插入数据库的数据
    */
  def parse(mapJson: Map[String, AnyVal], identifier: Int): ListBuffer[ListBuffer[String]] = {

    var outList = new ListBuffer[ListBuffer[String]]()

    //最外层的键值
    val ids = mapJson.keys

    ids.foreach {

      index => {

        //最里层的ListBuffer[] 得到一行的数据
        var inList = new ListBuffer[String]()
        //每个行的数据
        val values = mapJson.getOrElse(index, "")
        inList += index.toString //在每一行数据之前先添加第一列的key值

        if (values.toString.nonEmpty) {

          val sub = values.asInstanceOf[Map[String, AnyVal]]
          val keys = sub.keys

          keys.foreach {

            y => {

              var result = sub.getOrElse(y, "")

              if (result.toString.startsWith("--")) {
                result = ""
              }

              if (identifier == 0) {

                if (result.toString.contains(",")) {
                  result = result.toString.replace(",", "")
                }

              }

              //对于result 为"--"的情况
              inList += result.toString

            }

          }

        }

        outList += inList

      }

    }

    outList
  }

}
