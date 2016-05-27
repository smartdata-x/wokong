package com.kunyan.data.database

import java.util.Properties

import com.kunyan.data.scheduler.FollowScheduler

/**
  * Created by C.J.YOU on 2016/5/27.
  * MySql 操作类
  */
object MysqlOperation {

  val connection = "jdbc:mysql://:3306/stock_backup?user=root&password=dataservice2015&useUnicode=true&characterEncoding=utf8"
  val followTable = "follow_stock"
  val searchTable = "search_stock"
  val visitTable = "visit_stock"
  val  properties = new Properties()
  properties.setProperty("driver","com.mysql.jdbc.Driver")

  def getTableData(tableName:String): Unit = {
    FollowScheduler.sqlContext.read.jdbc(connection,tableName,properties).foreach(row =>{
      FollowScheduler.stockCodes.add(row(0).toString)
    })
  }
}
