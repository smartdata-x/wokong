package com.kunyan.personas.database

import java.sql.Connection
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
// "jdbc:mysql://:3306/jindowin?user=personas&password=personas&useUnicode=true&characterEncoding=utf8"
/**
  * Created by C.J.YOU on 2016/4/26.
  */
object Table extends  Serializable{

  @volatile private var instance: Broadcast[DataFrame] = null

  // 广播变量，为了避免 driver fail，用了单件模式
  def getInstance(sqlc:SQLContext): Broadcast[DataFrame] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val df = Table.getTableData(sqlc,"jdbc:mysql://222.73.34.91:3306/personas?user=personas&password=personas&useUnicode=true&characterEncoding=utf8","main_index")
          instance = sqlc.sparkContext.broadcast(df)
        }
      }
    }
    instance
  }

  def resetInstance(): Unit ={
    instance = null
  }
  // readData
  def getTableData(sqlc:SQLContext,readConnection:String,tableName:String): DataFrame ={
    val properties = new Properties()
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    val res = sqlc.read.jdbc(readConnection,tableName,properties).toDF("id","phone","qq","weibo")
    res
  }

  // writeData
  def writeTableData(dataFrame: DataFrame,writeConnection:String,tableName:String): Unit ={
    val properties = new Properties()
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    dataFrame.write.mode(SaveMode.Append).jdbc(writeConnection,tableName,properties)
  }

  // judge
  def isExist(col:String,param: String,conn:Connection): (Int,String,String,String) = {
    val st = conn.createStatement
    var id = -1
    var item1 =""
    var item2 =""
    var item3 =""
    val rs = st.executeQuery(String.format("select id,phone,qq,weibo from main_index where "+ col+" =\"%s\"",param))
    while (rs.next()) {
      id = rs.getInt(1)
      item1 = rs.getString(2)
      item2 = rs.getString(3)
      item3 = rs.getString(4)
    }
    (id,item1,item2,item3)
  }

  // get already exist item in db
  def getExistItem(col:Array[String],param: String,conn:Connection):(String,String)={
    val statement = conn.createStatement()
    var item1 =""
    var item2 =""
    val rs = statement.executeQuery(String.format("select "+col(0) +","+col(1) +" from main_index where id =\"%s\"",param))
    while (rs.next()) {
       item1 = rs.getString(1)
       item2 = rs.getString(2)
    }
    (item1,item2)
  }

}
