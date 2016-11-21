/**
  * Copyright @ 2015 ShanghaiKunyan. All rights reserved
  *
  * @author : Sunsolo
  *         Email       : wukun@kunyan-inc.com
  *         Date        : 2016-05-19 16:52
  *         Description :
  */
package com.kunyan.wokongsvc.realtimedata

import java.sql.Connection

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import logger.HeatLogger

/**
  * Created by wukun on 2016/5/18
  * mysql句柄池
  */
class MysqlPool private(val xmlHandle: XmlHandle, val mysqlSign: String = "stock") extends Serializable {

  try {
    Class.forName(xmlHandle.getElem("mysql", "driver"))
  } catch {
    case e: Exception => {
      HeatLogger.exception(e)
      System.exit(-1)
    }
  }

  lazy val config = createConfig

  lazy val connPool = new BoneCP(config)

  /**
    * 初始化连接池配置
    *
    * @author wukun
    */
  def createConfig: BoneCPConfig = {

    val initConfig = new BoneCPConfig

    if (!(mysqlSign == "stock" || mysqlSign == "test" || mysqlSign == "other_stock")) {
      HeatLogger.warn("couldn't obtain mysql conn by mysql sign")
    } else {
      initConfig.setJdbcUrl(xmlHandle.getElem("mysql_" + mysqlSign, "url"))
      initConfig.setUsername(xmlHandle.getElem("mysql_" + mysqlSign, "user"))
      initConfig.setPassword(xmlHandle.getElem("mysql_" + mysqlSign, "password"))
    }

    initConfig.setMinConnectionsPerPartition(Integer.parseInt(xmlHandle.getElem("mysql", "minconn")))
    initConfig.setMaxConnectionsPerPartition(Integer.parseInt(xmlHandle.getElem("mysql", "maxconn")))
    initConfig.setPartitionCount(Integer.parseInt(xmlHandle.getElem("mysql", "partition")))
    initConfig.setConnectionTimeoutInMs(Integer.parseInt(xmlHandle.getElem("mysql", "timeout")))
    initConfig.setConnectionTestStatement("select 1")
    initConfig.setIdleConnectionTestPeriodInMinutes(Integer.parseInt(xmlHandle.getElem("mysql", "connecttest")))

    initConfig
  }

  def setConfig(mix: Int, max: Int, testPeriod: Long) {
    config.setPartitionCount(1)
    config.setMinConnectionsPerPartition(mix)
    config.setMaxConnectionsPerPartition(max)
    config.setIdleConnectionTestPeriodInMinutes(3)
    config.setIdleMaxAgeInMinutes(3)
  }

  /**
    * 获取连接
    *
    * @author wukun
    */
  def getConnect: Option[Connection] = {

    var connect: Option[Connection] = null

    try {
      connect = Some(connPool.getConnection)
    } catch {

      case e: Exception => {

        if (connect != null) {
          connect.get.close
        }
        connect = None
      }
    }

    connect
  }

  def close {
    connPool.shutdown
  }
}

/**
  * Created by wukun on 2016/5/18
  * MysqlPool伴生对象
  */
object MysqlPool extends Serializable {

  def apply(xmlHandle: XmlHandle, mysqlSign: String = "stock"): MysqlPool = {
    new MysqlPool(xmlHandle, mysqlSign)
  }

}

