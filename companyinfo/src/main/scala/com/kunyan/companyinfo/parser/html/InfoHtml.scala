package com.kunyan.companyinfo.parser.html

import net.minidev.json.JSONObject
import org.jsoup.Jsoup
import org.jsoup.select.Elements

/**
  * Created by niujiaojiao on 2016/7/21.
  */
//两部分：基本资料,发行相关
object InfoHtml {

  /**
    * 公司概况解析入口
    * 写入hbase 表的字符串集合
    *
    * @param stockCode 公司股票代码
    * @return 返回总体的json字符串
    */
  def parseCpnyInstructure(stockCode: String): String = {

    var json = ""
    val map = new java.util.HashMap[String, Object]()

    if (stockCode.nonEmpty) {

      val doc = Jsoup.connect("http://f10.eastmoney.com/f10_v2/CompanySurvey.aspx?code=" + stockCode).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.120 Safari/535.2").timeout(20000).get()

      //基本资料
      var tableTop = doc.select("table#Table0 tbody")

      if (tableTop.toString.isEmpty) {
        tableTop = null
      }

      //发行相关
      var tableBot = doc.getElementById("fxxg").nextElementSibling().getElementsByTag("table").select("tbody")

      if (tableBot.toString.isEmpty) {
        tableBot = null
      }

      val mapTop = getJson(tableTop)
      val mapBot = getJson(tableBot)
      map.put("基本资料", mapTop)
      map.put("发行相关", mapBot)
      json = JSONObject.toJSONString(map)

    }

    json
  }

  /**
    * 传入table tbody一个对象
    *
    * @param children table tbody 对象（键值一一对应的表格）
    * @return
    */
  def getJson(children: Elements): java.util.HashMap[String, Object] = {

    val map = new java.util.HashMap[String, Object]()

    if (children != null) {

      val keys = children.first().getElementsByTag("th")
      val values = children.first().getElementsByTag("td")
      assert(keys.size == values.size)

      for (i <- 0 until keys.size) {
        map.put(keys.get(i).text, values.get(i).text)
      }

    }

    map
  }

}
