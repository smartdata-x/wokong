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

import MixTool.{Tuple2Map, TupleHashMapSet}

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
import scala.collection.mutable.ArrayBuffer

/**
  * Created by wukun on 2016/5/19
  * Mysql操作句柄类
  */
class MysqlHandle(conn: Connection) extends Serializable with CustomLogger {

  type TryHashMap = Try[HashMap[String, (String, String)]]

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
  def execQueryStockAlias(sql: String): Try[Tuple2Map] = {

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
      stockCode  -= "000001"
      stockChina -= "000001"
      stockJian  -= "000001"
      stockQuan  -= "000001"
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

  def execHyGnDiff(sql: String): Try[Unit] = {

    Try({
      val stmt = dbConn.createStatement
      stmt.execute(sql)
      stmt.close
    })
  }

  /**
    * 存储过程调用接口，以后会用到
    * @param  sql sql语句
    * @param  code 股票代码
    * @param  tamp 时间戳
    * @param  count 访问总数
    * @param  diff  访问差值
    * @param  table 表名
    * @param  day 第几天
    * @author wukun
    */
  def execADataProc(
    sql  : String,
    code : String, 
    tamp : Long, 
    count: Long, 
    diff : Long, 
    table: String, 
    day  : Int): Try[Unit] = {

    Try({

      val proc = dbConn.prepareCall(sql)

      proc.setString(1, code)
      proc.setString(5, table)
      proc.setLong(2, tamp)
      proc.setLong(3, count)
      proc.setLong(4, diff)
      proc.setInt(6, day)
      proc.execute
      proc.close

    })
  }

  /**
    * 存储过程调用接口，以后会用到
    * @param  sql sql语句
    * @param  name 行业名称
    * @param  tamp 时间戳
    * @param  count 访问总数
    * @param  diff  访问差值
    * @param  table 表名
    * @param  day 第几天
    * @param  distinct 月份表标识
    * @author wukun
    */
  def execHyGnDataProc(
    sql  : String,
    name : String, 
    tamp : Long, 
    count: Long, 
    diff : Long, 
    table: String,
    day  : Int,
    distinct: Int): Try[Unit] = {

    Try({

      val proc = dbConn.prepareCall(sql)

      proc.setString(1, name)
      proc.setLong(2, tamp)
      proc.setLong(3, count)
      proc.setLong(4, diff)
      proc.setString(5, table)
      proc.setInt(6, day)
      proc.setInt(7, distinct)
      proc.execute
      proc.close

    })
  }

  /**
    * 存储过程调用接口，以后会用到
    * @param  sql sql语句
    * @param  tamp 时间戳
    * @param  total 访问总数
    * @author wukun
    */
  def execTotalTimeProc(
    sql: String,
    tamp: Long,
    total: Long): Try[Unit] = {

    Try({

      val proc = dbConn.prepareCall(sql)

      proc.setLong(1, tamp)
      proc.setLong(2, total)
      proc.execute
      proc.close

    })
  }

  /**
    * 执行初始化操作
    * @param  sql sql语句
    * @author wukun
    */
  def execInitProc(sql: String): Try[Unit] = {

    Try({

      val proc = dbConn.prepareCall(sql)

      proc.execute
      proc.close

    })
  }

  /**
    * 执行差值计算
    * @param  sql sql语句
    * @author wukun
    */
  def execDiffInit(sql: String): Try[Unit] = {

    Try({

      val proc = dbConn.prepareCall(sql)

      proc.execute
      proc.close
    })
  }

  /**
    * 执行行业和概念查询操作
    * @param  sql sql语句
    * @author wukun
    */
  def execQueryHyGn(sql: String): Try[TupleHashMapSet] = {

    val ret = Try({

      val stmt = dbConn.createStatement
      val allInfo = stmt.executeQuery(sql)
      val col = allInfo.getMetaData().getColumnCount

      val stockHy = new HashMap[String, ListBuffer[String]]
      val stockGn = new HashMap[String, ListBuffer[String]]

      val hy = new HashSet[String]
      val gn = new HashSet[String]

      while (allInfo.next && col == 3) {

        val code = allInfo.getString(1)
        val hyInfos = allInfo.getString(2).split(",")

        if(hyInfos.size > 0) {

          stockHy += ((code, new ListBuffer[String]))

          for(hyInfo <- hyInfos) {

            if(hyInfo.compareTo("null") != 0) {

              stockHy(code) += hyInfo
              hy += hyInfo

            }
          }

        }

        val gnInfos = allInfo.getString(3).split(",")

        if(gnInfos.size > 0) {

          stockGn += ((code, new ListBuffer[String]))

          for(gnInfo <- gnInfos) {

            if(gnInfo.compareTo("null") != 0) {

              stockGn(code) += gnInfo
              gn += gnInfo

            }
          }

        }
      }

      stmt.close
      ((stockHy, stockGn), (hy, gn))
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

