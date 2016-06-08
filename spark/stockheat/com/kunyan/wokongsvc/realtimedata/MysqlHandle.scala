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
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Created by wukun on 2016/5/19
  * Mysql操作句柄类
  */
class MysqlHandle(conn: Connection) extends Serializable with CustomLogger {

  type TryHashMap = Try[HashMap[String, (String, String)]]
  type TryTuple3HashMap = Try[(HashMap[String, String], HashMap[String, String], HashMap[String, String])]

  private var dbConn = conn

  def close {
    dbConn.close();
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
      val sqlInfo = (xml.getElem("mySql", "user"), xml.getElem("mySql", "password"))
      Class.forName(xml.getElem("mySql", "driver"))
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

  def execQueryStockAlias(sql: String): TryTuple3HashMap = {

    val ret = Try({

      val stmt = dbConn.createStatement
      val allInfo = stmt.executeQuery(sql)
      val col = allInfo.getMetaData().getColumnCount

      val stockChina = new HashMap[String, String]
      val stockJian = new HashMap[String, String]
      val stockQuan = new HashMap[String, String]

      while (allInfo.next && col == 4) {
        val stockCode = allInfo.getString(1)
        stockChina += (allInfo.getString(2) -> stockCode)
        stockJian += (allInfo.getString(3).toUpperCase -> stockCode)
        stockQuan += (allInfo.getString(4).toUpperCase -> stockCode)
      }

      stmt.close
      (stockChina, stockJian, stockQuan)
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

}

