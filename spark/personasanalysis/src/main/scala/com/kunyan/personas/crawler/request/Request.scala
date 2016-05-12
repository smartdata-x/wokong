package com.kunyan.personas.crawler.request

import java.util

import org.jsoup.Connection.Method
import org.jsoup.Jsoup

/**
  * Created by C.J.YOU on 2016/5/12.
  */
object Request {

  //根据cookie字符串获取cookie的map
  private  def getCookies(cookieStr: String): util.HashMap[String, String] = {
    val cookieMap = new util.HashMap[String, String]()
    val cookieArr = cookieStr.split(";")
    for (line <- cookieArr) {
      val lineArr = line.split("=")
      if (lineArr.length > 1) {
        cookieMap.put(lineArr(0), lineArr(1))
      }
    }
    cookieMap
  }

  private def requestForUid(ua:String,weiboId:String,cookie:String): String = {
    val url = "http://weibo.com/u/"+weiboId+"?retcode=6102"
    var uid = ""
    try {
      val response = Jsoup.connect(url)
        .userAgent(ua)
        .method(Method.GET)
        .timeout(3000)
        .cookies(getCookies(cookie))
        .followRedirects(true)
        .execute()

       response.body()
       .split("\\$CONFIG").foreach(f => {

        if (f.contains("['page_id']")) {

          uid = f.replace(";", "").split("=")(1).replace("'", "").trim
        }
      })
    } catch {
      case exception: Exception => println(weiboId+":requestForUid time out")
    }

    uid

  }

  def requestForUserInfo(uid:String,ua:String,cookie:String): String ={
    println("requestForUserInfo:"+uid)
    var res = ""
    try {
      // http://weibo.com/p/1005055788957514/info?mod=pedit_more&retcode=6102
      val url = "http://weibo.com/p/" + uid + "/info?mod=pedit_more&retcode=6102"
      if(uid.nonEmpty){
        val response = Jsoup.connect(url)
          .userAgent(ua)
          .method(Method.GET)
          .timeout(5000)
          .cookies(getCookies(cookie))
          .followRedirects(true)
          .execute()
        res = response.body()
      }
    } catch {
      case exception: Exception => println("requestForUserInfo read out")
    }
    res
  }

  def sendRequest(ua:String,weiboId :String,cookie:String): String ={
    val uid = requestForUid(ua,weiboId,cookie)
    val response = requestForUserInfo(uid,ua,cookie)
    val info = extractorInfo(weiboId,response)
    info
  }

  private def parserInfo(infoName: String, infoStr: String): String = {
    if (infoName == "公司") {
      val workInfo = infoStr.split("<\\\\/span>")(1)
      val company = workInfo.split("<\\\\/a>")(0).split(">").last
      var position = ""
      if (workInfo.contains("职位")) {
        position = workInfo.split("职位：").last
      }
       company + "=" + position
    } else {
      val anyInfo = infoStr.split("<\\\\/span>")(1).split(">").last
      if (anyInfo.contains("pt_detail")) {
         "NoDef"
      } else {
         anyInfo
      }
    }
  }

  private def extractorInfo(weiboId:String,body:String):String ={
    //用户信息
    var userInfo = ""
    //微博账号
    val weiBoId = weiboId
    //QQ
    var QQ = "NoDef"
    //邮箱
    var email = "NoDef"
    //职业(不可获得)
    val job = "NoDef"
    //身份
    var position = "NoDef"
    //真是姓名(不可获得)
    val realName = "NoDef"
    //公司
    var company = "NoDef"
    //地址（所在地）
    var address = "NoDef"

    println(body)

    if(body.nonEmpty){

      for (x <- body.split("<script>FM.view")) {
        if (x.contains("\"ns\":\"\",\"domid\":\"Pl_Official_PersonalInfo__62\"")) {
          val data = x.replace("\\t", "").replace("\\n", "").replace("\\r", "")
          val datas = data.split("<span class=\\\\\"pt_title S_txt2\\\\\">")
          datas.foreach(d => {
            //获取QQ信息
            if (d.contains("QQ")) {
              QQ = parserInfo("QQ", d)
            }
            //获取邮箱
            if (d.contains("邮箱")) {
              email = parserInfo("邮箱", d)
            }
            //获取所在地
            if (d.contains("所在地")) {
              address = parserInfo("所在地", d)
            }
            //获取公司和身份信息
            if (d.contains("公司")) {
              val workInfo = parserInfo("公司", d).split("=")
              company = workInfo(0)
              if (workInfo.length == 2) {
                position = workInfo(1)
              }
            }
          })
        }
      }
    }
    userInfo = weiBoId + "-->" + QQ + "-->" + email + "-->" + job + "-->" + position + "-->" + realName + "-->" + company + "-->" + address
    userInfo
  }

}
