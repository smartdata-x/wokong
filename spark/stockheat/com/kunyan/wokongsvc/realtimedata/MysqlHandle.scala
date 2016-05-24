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
class MysqlHandle(conn: Connection) extends Serializable {

  private[this] var dbConn = conn

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
        println(e.getMessage)
        System.exit(-1)
      }

      case e: ClassNotFoundException => {
        println(e.getMessage)
        System.exit(-1)
      }

      case e: Exception => {
        println(e.getMessage)
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
  def execQuery(sql: String): Try[HashMap[String, (String, String)]] = {
    
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






