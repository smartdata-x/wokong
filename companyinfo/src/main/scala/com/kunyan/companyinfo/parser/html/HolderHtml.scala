package com.kunyan.companyinfo.parser.html

import net.minidev.json.JSONObject
import org.jsoup.Jsoup
import org.jsoup.select.Elements


/**
  * Created by zhangruibo on 2016/7/21.
  * update by niujiaojiao on 2016/7/23.
  * 股东研究
  */
object HolderHtml {

  /**
    * 股东研究模块入口
    * 写入hbase 表的字符串集合
    * @param stockCode 股票代码
    * @return 整个板块的字符串
    */
  def parseStockHolders(stockCode: String): String = {

    var json = ""
    val map = new java.util.HashMap[String, Object]()

    if (stockCode.isEmpty) {

      return ""

    } else {

      val doc = Jsoup.connect("http://f10.eastmoney.com/f10_v2/ShareholderResearch.aspx?code=" + stockCode).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.120 Safari/535.2").timeout(20000).get()

      //十大流通股东
      var tableTop = new Elements()

      if (doc.toString.contains("TTCS_Table_Div")) {

        tableTop = doc.getElementById("TTCS_Table_Div").select("table tbody")

      } else {

        tableTop = null

      }

      var partOneDates = new Elements()

      if (tableTop != null) {

        if (doc.toString.contains("sdltgd")) {

          partOneDates = doc.getElementById("sdltgd").nextElementSibling().getElementsByTag("li")

        } else {

          partOneDates = null

        }

      }

      //十大股东

      var tableBot = new Elements()

      if (doc.toString.contains("TTS_Table_Div")) {

        tableBot = doc.getElementById("TTS_Table_Div").select("table tbody")

      } else {

        tableBot = null

      }

      var partTwoDates = new Elements()

      if (tableBot != null) {

        if (doc.toString.contains("sdgd")) {

          partTwoDates = doc.getElementById("sdgd").nextElementSibling().getElementsByTag("li")

        } else {

          partTwoDates = null

        }

      }

      val mapTop = getJson(tableTop, partOneDates)
      val mapBot = getJson(tableBot, partTwoDates)
      map.put("十大流通股东", mapTop)
      map.put("十大股东", mapBot)
      json = JSONObject.toJSONString(map)

    }

    json
  }

  /**
    * 获取相对应的map 集合
    *
    * @param children 所有的对应的日期的表的字符串：分析以日期字符串对象的个数为基准，表的字符串的个数不符合需求（始终是五个）
    * @param allDates 所有的日期的字符串
    * @return map 集合
    */
  def getJson(children: Elements, allDates: Elements): java.util.HashMap[String, Object] = {

    val map = new java.util.HashMap[String, Object]()

    if (children != null && allDates != null) {

      for (i <- 0 until allDates.size) {

        val tr = children.get(i).select("tr")
        val thkeys = tr.first().getElementsByTag("th")
        val mapOut = new java.util.HashMap[String, Object]()

        for (j <- 1 until tr.size) {

          val subvalue = tr.get(j).getElementsByTag("th")
          val values = tr.get(j).getElementsByTag("td")
          val mapIn = new java.util.HashMap[String, Object]()

          for (k <- 0 until thkeys.size) {

            if (k == 0) {
              mapIn.put(thkeys.get(0).text(), subvalue.text)
            } else {

              if(k<= values.size){
                mapIn.put(thkeys.get(k).text(), values.get(k - 1).text)
              }else {
                mapIn.put(thkeys.get(k).text(),"")
              }

            }

          }

          mapOut.put(j.toString, mapIn)

        }

        map.put(allDates.get(i).text, mapOut)

      }

    }

    map
  }

}
