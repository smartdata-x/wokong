/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /opt/spark-1.2.2-bin-hadoop2.4/work/spark/html_jave/src/main/java/com/kunyan/wokongsvc/mail/MySqlTest.java
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-05-19 20:04
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import java.sql.CallableStatement
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.sql.DriverManager
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.collection.mutable.ListBuffer

/**
  * Created by wukun on 2016/5/19
  * Mysql操作句柄类
  */
class MysqlHandle(conn: Connection) extends Serializable with CustomLogger {

  type TryHashMap = Try[HashMap[String, (String, String)]]
  type TupleHashMap = (HashMap[String, ListBuffer[String]], HashMap[String, ListBuffer[String]])
  type TryTuple3HashMap = Try[(HashSet[String], (HashMap[String, String], HashMap[String, String], HashMap[String, String]))]

  private var dbConn = conn

  private val stateMent: Statement = dbConn.createStatement

  def close {

    stateMent.close()
    dbConn.close()

  }

  /**
    * 另一种初始化方法
    * @param url mysql配置的url地址
    * @param xml 全局XML句柄
    * @author wukun
    */
  def this(url: String, xml: XmlHandle) {

    this(null)

    try {

      val sqlInfo = (xml.getElem("mySql", "userstock"), xml.getElem("mySql", "passwordstock"))
      // 这个方法可以不必显示调用，判断标准为jar包的META-INF/services/目录的java.sql.Driver文件里是否包含
      // com.mysql.jdbc.Driver这行，在DriverManager被加载时的静态块中会遍历这个文件里的内容进行主动加载
      // Class.forName(xml.getElem("mySql", "driver"))
      dbConn = DriverManager.getConnection(url, sqlInfo._1, sqlInfo._2)

    } catch {

      case e: SQLException => {
        errorLog(fileInfo, e.getMessage + "[SQLException]")
        System.exit(-1)
      }

      case e: ClassNotFoundException => {
        errorLog(fileInfo, e.getMessage + "[ClassNotFoundException]")
        System.exit(-1)
      }

      case e: Exception => {
        errorLog(fileInfo, e.getMessage)
        System.exit(-1)
      }
    }
  }

  def batchExec(): Try[Array[Int]] = {

    val ret = Try(stateMent.executeBatch)

    ret
  }

  def addCommand(sql: String): Try[Unit] = {

    val ret = Try(stateMent.addBatch(sql))

    ret
  }

  /**
    * 执行插入操作
    * @param  sql sql语句
    * @author wukun
    */
  def execInsertInto(sql: String): Try[Int] = {

    val ret = Try({

      val stmt = dbConn.createStatement
      val count = stmt.executeUpdate(sql)
      stmt.close
      count
    })

    ret
  }

  /**
    * 执行更新操作
    * @param  sql sql语句
    * @author wukun
    */
  def execUpdate(sql: String): Try[Int] = {

    val ret = Try({

      val stmt = dbConn.createStatement
      val count = stmt.executeUpdate(sql)
      stmt.close
      count
    })

    ret
  }

  /**
    * 执行查询操作
    * @param  sql sql语句
    * @author wukun
    */
  def execQuerySyn(sql: String): TryHashMap = {
    
    val ret = Try({

      val stmt = dbConn.createStatement
      val allInfo = stmt.executeQuery(sql)
      val col = allInfo.getMetaData().getColumnCount
      val stockToHyOrGn = new HashMap[String, (String, String)]

      while (allInfo.next && col == 3) {
        stockToHyOrGn += (allInfo.getString(1) -> (allInfo.getString(2), allInfo.getString(3)))
      }

      stmt.close
      stockToHyOrGn
    })

    ret
  }

  /**
    * 执行股票别名查询操作
    * @param  sql sql语句
    * @author wukun
    */
  def execQueryStockAlias(sql: String): TryTuple3HashMap = {

    val ret = Try({

      val stmt = dbConn.createStatement
      val allInfo = stmt.executeQuery(sql)
      val col = allInfo.getMetaData().getColumnCount

      val stockCode = new HashSet[String]
      val stockChina = new HashMap[String, String]
      val stockJian = new HashMap[String, String]
      val stockQuan = new HashMap[String, String]

      while (allInfo.next && col == 4) {
        val code = allInfo.getString(1)
        stockCode.add(code)
        stockChina += (allInfo.getString(2) -> code)
        stockJian += (allInfo.getString(3).toUpperCase -> code)
        stockQuan += (allInfo.getString(4).toUpperCase -> code)
      }

      stmt.close
      stockCode -= "000001"
      stockChina -= "000001"
      stockJian -= "000001"
      stockQuan -= "000001"
      (stockCode, stockChina.filter( x => x._2.compareTo("000001") != 0))
      (stockCode, (stockChina, stockJian, stockQuan))
    })

    ret
  }

  /**
    * 执行股票代码查询操作
    * @param  sql sql语句
    * @author wukun
    */
  def execQueryStock(sql: String): Try[HashSet[String]] = {

    val ret = Try({

      val stmt = dbConn.createStatement
      val allInfo = stmt.executeQuery(sql)
      val col = allInfo.getMetaData().getColumnCount

      val stockCode = new HashSet[String]

      while (allInfo.next && col == 1) {
        stockCode.add(allInfo.getString(1))
      }

      stmt.close
      stockCode -= "000001"
      stockCode
    })

    ret
  }

  /**
    * 执行股票代码到中文名映射的查询操作
    * @param  sql sql语句
    * @author wukun
    */
  def execQueryStockInfo(sql: String): Try[HashMap[String, String]] = {

    val ret = Try({

      val stmt = dbConn.createStatement
      val allInfo = stmt.executeQuery(sql)
      val col = allInfo.getMetaData().getColumnCount

      val stockInfo = new HashMap[String, String]

      while (allInfo.next && col == 2) {
        stockInfo += ((allInfo.getString(1), allInfo.getString(2)))
      }

      stmt.close
      stockInfo
    })

    ret
  }

  /**
    * 存储过程调用接口，以后会用到
    * @param  sql sql语句
    * @author wukun
    */
  def execProc(sql: String): Try[HashMap[String, String]] = {

    val ret = Try({

      val proc = dbConn.prepareCall(sql)
      proc.setInt(1, 5)
      proc.execute
      val allInfo = proc.getResultSet
      val col = allInfo.getMetaData().getColumnCount

      val stockInfo = new HashMap[String, String]

      while (allInfo.next && col == 2) {
        stockInfo += ((allInfo.getString(1), allInfo.getString(2)))
      }

      proc.close
      stockInfo
    })

    ret
  }

  /**
    * 执行行业和概念查询操作
    * @param  sql sql语句
    * @author wukun
    */
  def execQueryHyGn(sql: String): Try[TupleHashMap] = {

    val ret = Try({

      val stmt = dbConn.createStatement
      val allInfo = stmt.executeQuery(sql)
      val col = allInfo.getMetaData().getColumnCount

      val stockHy = new HashMap[String, ListBuffer[String]]
      val stockGn = new HashMap[String, ListBuffer[String]]

      while (allInfo.next && col == 3) {

        val code = allInfo.getString(1)
        val hyInfos = allInfo.getString(2).split(",")
        if(hyInfos.size > 0) {
          stockHy += ((code, new ListBuffer[String]))
          for(hyInfo <- hyInfos) {
            stockHy(code) += (hyInfo)
          }
        }

        val gnInfos = allInfo.getString(3).split(",")
        if(gnInfos.size > 0) {
          stockGn += ((code, new ListBuffer[String]))
          for(gnInfo <- gnInfos) {
            stockGn(code) += (gnInfo)
          }
        }
      }

      stmt.close
      (stockHy, stockGn)
    })

    ret
  }
}

/** 
  * Created by wukun on 2016/5/19
  * MysqlHandle伴生对象
  */
object MysqlHandle {

  def apply(connect: Connection): MysqlHandle = {
    new MysqlHandle(connect)
  }

  def apply(
    url: String, 
    xml: XmlHandle
  ): MysqlHandle = {
    new MysqlHandle(url, xml)
  }

  def main(args: Array[String]) {

    val xml = XmlHandle("./config.xml")
    val url = xml.getElem("mySql", "totalurl")
    val sqlHandle = MysqlHandle(url, xml)

    val ret = sqlHandle.execProc("{call query_stock_info(?)}")
    ret match {
      case Success(e) => println(e)
      case Failure(e) => println(e)
    }
  }
}

