package com.kunyan.companyinfo.parser.json

import scala.util.parsing.json.JSON

/**
  * Created by niujiaojiao on 2016/7/28.
  * 公司概况
  */
object InfoJson {

  def parse(totalJson: String): Map[String, AnyVal] = {

    val map = JSON.parseFull(totalJson)

    if (map.isEmpty) {

      println("\"JSON parse value is empty,please have a check!\"")

    } else {

      try {
        return map.asInstanceOf[Map[String, AnyVal]].getOrElse("基本资料", "").asInstanceOf[Map[String, AnyVal]]
      } catch {
        case e:Exception =>
          e.printStackTrace()
      }

    }

    null
  }

}
