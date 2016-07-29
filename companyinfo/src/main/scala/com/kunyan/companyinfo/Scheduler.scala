package com.kunyan.companyinfo

import java.sql.{Connection, DriverManager}

import com.kunyan.companyinfo.db.HbaseConnection
import com.kunyan.companyinfo.parser.json._
import org.apache.hadoop.hbase.HBaseConfiguration

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.xml.XML

/**
  * Created by niujiaojiao on 2016/7/21.
  * 主程序入口
  */
object Scheduler {

  /**
    * 主程序操作 四个部分 八个表写入数据库，从hbase表的相应表名读取字符串
    *
    */
  def main(args: Array[String]): Unit = {

    val configFile = XML.loadFile(ClassLoader.getSystemResource("configFile.xml").toString.split("file:/")(1))

    //hbase 连接信息
    val hbaseConf = HBaseConfiguration.create
    hbaseConf.set("hbase.rootdir", (configFile \ "hbase" \ "rootDir").text)
    hbaseConf.set("hbase.zookeeper.quorum", (configFile \ "hbase" \ "ip").text)
    val hbaseConnection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConf)

    val mysqlConnection = DriverManager.getConnection((configFile \ "mysql" \ "url").text,
      (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)
    mysqlConnection.setAutoCommit(true)

    try {

      //股票代码
      val lines = Source.fromFile(ClassLoader.getSystemResource("StockCode.txt").toString.split("file:/")(1)).getLines()

      lines.foreach {

        x => {

          val companyResult = HbaseConnection.query(HbaseConnection.TABLE_NAME, x, hbaseConnection)

          companyInfo(mysqlConnection, x, companyResult)
          companyLeader(mysqlConnection, x, companyResult)
          shareStruct(mysqlConnection, x, companyResult)
          shareHolder(mysqlConnection, x, companyResult)

        }

      }

    } catch {

      case e: Exception =>
        e.printStackTrace()

    } finally {

      hbaseConnection.close()
      mysqlConnection.close()

    }

  }

  def shareHolder(mysqlConnection: Connection, x: String, companyResult: (String, String, String, String)): Unit = {

    if (null != companyResult._4) {

      // two tables:float_stockholder and stockholder
      val result = HolderJson.parse(companyResult._4)
      val floatSql = mysqlConnection.prepareStatement("INSERT INTO top10_float_stockholder (stock_code, date, rank, stockholder_name,stockholder_nature, share_type, shares_number, total_ratio, change_share,change_ratio)" +
        " VALUES (?,?,?,?,?,?,?,?,?,?)")
      val holderSql = mysqlConnection.prepareStatement("INSERT INTO top10_stockholder (stock_code, date, rank, stockholder_name, share_type, shares_number, total_ratio, change_share,change_ratio) VALUES (?,?,?,?,?,?,?,?,?)")
      val floatData = result._1
      val nonfloatData = result._2

      //十大流通股东
      for (i <- floatData.indices) {

        val topData = floatData(i)

        for (j <- topData.indices) {

          val midData = topData(j)
          val stockCode = x
          var date = ""
          var rank = 0
          var stockholderName = ""
          var stockholderNature = ""
          var shareType = ""
          var shareNumber = ""
          var totalRatio = ""
          var changeShare = ""
          var changeRatio = ""

          for (k <- midData.indices) {

            //这一层是指一行的数据stockholder_nature, share_type, share_number, total_ratio, change_share,change_ratio

            date = midData.head

            if (midData(7).nonEmpty) {
              rank = midData(7).toInt
            }

            stockholderName = midData(3)
            stockholderNature = midData(4)
            shareType = midData(1)
            shareNumber = midData(5)
            totalRatio = midData(2)
            changeShare = midData(6)
            changeRatio = midData(8)

          }

          HbaseConnection.insert(floatSql, stockCode, date, rank, stockholderName, stockholderNature, shareType, shareNumber, totalRatio, changeShare, changeRatio)

        }

      }

      //十大股东
      for (i <- nonfloatData.indices) {

        val topData = nonfloatData(i)

        for (j <- topData.indices) {

          val midData = topData(j)
          val stockCode = x
          var date = ""
          var rank = 0
          var stockholderName = ""
          var shareType = ""
          var shareNumber = ""
          var totalRatio = ""
          var changeShare = ""
          var changeRatio = ""

          for (k <- midData.indices) {

            //(2015-06-30, 流通A股,限售流通A股, 3.65%, 58,563,387, 157,487, 4, 0.27%)
            //这一层是指一行的数据stockholder_name, share_type, share_number, total_ratio, change_share,change_ratio

            date = midData.head

            if (midData(6).nonEmpty) {
              rank = midData(6).toInt
            }

            stockholderName = midData(3)
            shareType = midData(1)
            shareNumber = midData(4)
            totalRatio = midData(2)
            changeShare = midData(5)
            changeRatio = midData(7)

          }

          HbaseConnection.insert(holderSql, stockCode, date, rank, stockholderName, shareType, shareNumber, totalRatio, changeShare, changeRatio)

        }

      }

    }

  }

  def shareStruct(mysqlConnection: Connection, x: String, companyResult: (String, String, String, String)): Unit = {

    if (null != companyResult._3) {

      val result = StructJson.parse(companyResult._3)

      //股本结构 TableOne

      val stockLimitSql = mysqlConnection.prepareStatement("INSERT INTO stock_limit " +
        "(stock_code,case_name, value, percent)" +
        " VALUES (?,?,?,?)")

      result._1.foreach {

        info => {

          val stockCode = x
          val caseName = info.head
          var value = 0.0

          if (info(1).nonEmpty) {
            value = info(1).toDouble
          }

          val percent = info(2)
          var res = 0.toDouble

          if (percent.nonEmpty) {

            if (percent.contains("%")) {
              res = (percent.split("%")(0).toDouble / 100).formatted("%.4f").toDouble
            }

          }

          HbaseConnection.insert(stockLimitSql, stockCode, caseName, value, res)

        }

      }


      //Table two
      val stockCirculatingSql = mysqlConnection.prepareStatement("INSERT INTO stock_circulating " +
        "(stock_code,case_name, value, percent)" +
        " VALUES (?,?,?,?)")

      result._2.foreach {

        info => {

          val stockCode = x
          val stockName = info.head
          var value = 0.0

          if (info(1).nonEmpty) {
            value = info(1).toDouble
          }

          val percent = info(2)
          // 百分比要转换成小数
          var res = 0.toDouble

          if (percent.nonEmpty) {

            if (percent.contains("%")) {
              res = (percent.split("%")(0).toDouble / 100).formatted("%.4f").toDouble
            }

          }

          if (!stockName.startsWith("其它已流通股份")) {
            HbaseConnection.insert(stockCirculatingSql, stockCode, stockName, value, res)
          }

        }

      }

      //Table three
      val calendarStockSql = mysqlConnection.prepareStatement("INSERT INTO calendar_year_stock " +
        "(stock_code,date,general_capital, state_backing, limit_share," +
        "state_backing_limit, float_share, listed_share)" +
        " VALUES (?,?,?,?,?,?,?,?)")

      result._3.foreach {

        info => {

          val stockCode = x
          var date = ""

          if (info.nonEmpty) {
            date = info.last
          }

          var generalCapital = 0.0
          var limitShare = 0.0
          var floatShare = 0.0
          var listedShare = 0.0
          var stateBacking = 0.0
          var stateBackingLimit = 0.0

          for (j <- info.indices) {

            if (info(j).startsWith("总股本")) {

              generalCapital = info(j + 1).toString.toDouble

            } else if (info(j).startsWith("流通受限股份")) {

              if (info(j + 1).toString.nonEmpty) {
                limitShare = info(j + 1).toString.toDouble
              }

            } else if (info(j).startsWith("已流通股份")) {

              if (info(j + 1).toString.nonEmpty) {
                floatShare = info(j + 1).toString.toDouble
              }

            } else if (info(j).startsWith("已上市流通A股")) {

              if (info(j + 1).toString.nonEmpty) {
                listedShare = info(j + 1).toString.toDouble
              }

            } else if (info(j).startsWith("国家持股")) {

              if (info(j + 1).toString.nonEmpty) {
                stateBacking = info(j + 1).toString.toDouble
              }

            } else if (info(j).startsWith("国家持股(受限)")) {

              if (info(j + 1).toString.nonEmpty) {
                stateBackingLimit = info(j + 1).toString.toDouble
              }

            }

          }

          HbaseConnection.insert(calendarStockSql, stockCode, date, generalCapital, stateBacking, limitShare,
            stateBackingLimit, floatShare, listedShare)

        }

      }

    }

  }

  def companyLeader(mysqlConnection: Connection, x: String, companyResult: (String, String, String, String)): Unit = {

    if (null != companyResult._2) {

      val result = LeaderJson.parse(companyResult._2)

      val stockExecutiveSql = mysqlConnection.prepareStatement("INSERT INTO stock_executive " +
        "(stock_code,number,name,sex,age,education,duty)" +
        " VALUES (?,?,?,?,?,?,?)")
      val executivesProfileSql = mysqlConnection.prepareStatement("INSERT INTO executives_profile " +
        "(stock_code,name,sex,education,position,brief_intro)" +
        " VALUES (?,?,?,?,?,?)")
      //Table one
      result._1.foreach {

        info => {

          if (info != null && info.toString() != "") {

            val stockCode = x
            val number = info.head.toInt
            val name = info(1)
            val sex = info(4)
            var age = 0

            if (info(2) != "") {
              age = info(2).toInt
            }

            val education = info(3)
            val duty = info(5)

            HbaseConnection.insert(stockExecutiveSql, stockCode, number, name, sex, age, education, duty)

          }

        }

      }


      //Table two
      result._2.foreach {

        info => {

          if (info != null) {

            val stockCode = x
            val name = info.head
            val sex = info(1)
            val education = info(2)
            var position = ""

            if (info.size >= 4) {

              if (info(3).nonEmpty) {
                position = info(3)
              }

            }

            var briefIntro = ""

            if (info.size >= 5) {

              if (info(4).nonEmpty) {
                briefIntro = info(4)
              }

            }

            HbaseConnection.insert(executivesProfileSql, stockCode, name, sex, education, position, briefIntro)

          }

        }

      }

    }

  }

  def companyInfo(mysqlConnection: Connection, x: String, companyResult: (String, String, String, String)): Unit = {

    if (null != companyResult._1) {

      val map = InfoJson.parse(companyResult._1)
      val stockCode = x
      val companyName = map.getOrElse("公司名称", "").toString
      val companyEngName = map.getOrElse("英文名称", "").toString
      val usedName = map.getOrElse("曾用名", "").toString
      val aStockcode = map.getOrElse("A股代码", "").toString
      val aShort = map.getOrElse("A股简称", "").toString
      val bStockcode = map.getOrElse("B股代码", "").toString
      val bShort = map.getOrElse("B股简称", "").toString
      val hStockCode = map.getOrElse("H股代码", "").toString
      val hShort = map.getOrElse("H股简称", "").toString
      val securityType = map.getOrElse("证券类别", "").toString
      val industryInvolved = map.getOrElse("所属行业", "").toString
      val ceo = map.getOrElse("总经理", "").toString
      val lawPerson = map.getOrElse("法人代表", "").toString
      val secretary = map.getOrElse("董秘", "").toString
      val chairman = map.getOrElse("董事长", "").toString
      val securityAgent = map.getOrElse("证券事务代表", "").toString
      val independentDirector = map.getOrElse("独立董事", "").toString
      val companyTel = map.getOrElse("联系电话", "").toString
      val companyEmail = map.getOrElse("电子信箱", "").toString
      val companyFax = map.getOrElse("传真", "").toString
      val companyWebsite = map.getOrElse("公司网址", "").toString
      val businessAddress = map.getOrElse("办公地址", "").toString
      val regAddress = map.getOrElse("注册地址", "").toString
      val area = map.getOrElse("区域", "").toString
      val postCode = map.getOrElse("邮政编码", "").toString
      val regCaptical = map.getOrElse("注册资本(元)", "").toString
      val businessRegistration = map.getOrElse("工商登记", "").toString
      val employeeNum = map.getOrElse("雇员人数", "").toString
      val adminNum = map.getOrElse("管理人员人数", "").toString
      val lawFirm = map.getOrElse("律师事务所", "").toString
      val accountingFirm = map.getOrElse("会计师事务所", "").toString
      val companyIntro = map.getOrElse("公司简介", "").toString
      val businessScope = map.getOrElse("经营范围", "").toString

      val companyProfileStat = mysqlConnection.prepareStatement("INSERT INTO company_profile " +
        "(stock_code, company_name, company_eng_name, used_name, A_stockcode, A_short, B_stockcode, B_short, " +
        "H_stockcode, H_short,security_type, industry_involved, ceo, law_person, secretary, chairman, " +
        "security_agent, independent_director, company_tel, company_email, company_fax, company_website, " +
        "business_address, reg_address, area, post_code, reg_captial, business_registration, employee_num, " +
        "admin_num, law_firm, accounting_firm, company_intro, business_scope) " +
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

      HbaseConnection.insert(companyProfileStat, stockCode, companyName, companyEngName, usedName,
        aStockcode, aShort, bStockcode, bShort,
        hStockCode, hShort, securityType, industryInvolved, ceo,
        lawPerson, secretary, chairman, securityAgent, independentDirector,
        companyTel, companyEmail, companyFax, companyWebsite,
        businessAddress, regAddress, area, postCode, regCaptical,
        businessRegistration, employeeNum, adminNum, lawFirm,
        accountingFirm, companyIntro, businessScope)

    }

  }

  /**
    * 获取股票代码
    *
    * @param path 读取文件的路径
    * @return 所有股票代码的集合
    */
  def getCode(path: String): ListBuffer[String] = {

    var result = ListBuffer[String]()
    val file = Source.fromFile(path).mkString

    file.foreach(result += _.toString)

    result
  }

}
