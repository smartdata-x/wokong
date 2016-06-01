package com.kunyan.data.scheduler

import java.util

import com.kunyan.data.database.MysqlOperation
import com.kunyan.data.hbase.TableHbase
import com.kunyan.data.log.PrismLogger
import com.kunyan.data.util.{FileUtil, TimeUtil}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/5/27.
  * 获取hbase follow 数据的 主程序
  */
object ActiveUserFollow {

  var stockCodes:java.util.List[String] = new util.ArrayList[String]()

  def mapFunction(key:String,stockCodesTemp: mutable.HashSet[String]): String = {

    val stockFinalList = new ListBuffer[String]
    val info = key.split(":")
    val timeStamp = info(1)
    val userId = info(0)
    val hour = TimeUtil.getTime(timeStamp)
    /** follow */
    if (stockCodesTemp.nonEmpty  &&  timeStamp != null) {
      for (stockCode <- stockCodesTemp) {
        if(stockCodes.contains(stockCode)) {
          stockFinalList.+=(stockCode)
        }
      }
    }
    userId + "\t" + hour + "\t" + stockFinalList.mkString(",")
  }

  def keyUserDateMap(array: Array[String]): ((String,String),Set[String]) = {
    ((array(0),array(1).split(" ")(0)),array(2).split(",").toSet)
  }

  def keyUserMap(value:((String,String),Set[String])): (String,Set[String]) = {
    (value._1._1,value._2)
  }

  /**
    * 如何判断某个用户是否为活跃用户
    * 最小关注量 与 各个时间关注的不同自选股的总量之差大于零的
    * 获取给定时间段内，每天用户自选股最大集合
    * @param one 一天的自选股set
    * @param two 第二天的自选股set
    * @return 最大的Set集合
    */
  def getMaxSize(one: Set[String], two: Set[String]): Set[String] = {

    if(one.size > two.size) one else two

  }

  /**
    * 获取给定时间段内，每天用户自选股最小集合
    * @param one 一天的自选股set
    * @param two 第二天的自选股set
    * @return 最小的Set集合
    */
  def getMinSize(one: Set[String], two: Set[String]): Set[String] = {

    if(one.size <= two.size) one else two

  }

  val sparkConf = new SparkConf()
    .setAppName("ActiveUserFollow")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "2000")
    .setMaster("local")

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  def getTableData(tableName:String): Unit = {
    sqlContext.read.jdbc(MysqlOperation.connection,tableName,MysqlOperation.properties).foreach(row =>{
      stockCodes.add(row(0).toString)
    })
  }

  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Usage: LoadData files ")
      System.exit(-1)
    }

    // getTableData("stock_info")

    try{

      val data  = sc.textFile(args(0))

      println("data count:" + data.count())

      val result = data.map(_.split("\t")).filter(_.length == 3).map(keyUserDateMap)
          .reduceByKey(_.union(_)).map(keyUserMap)

      println("result:" + result.count())

      val userInfo = result.reduceByKey(_.union(_)).map(x =>(x._1,(x._2,x._2.size)))

      println("userInfo:" + userInfo.count())

      val dateUserInfo = result.reduceByKey(getMinSize).map(x => (x._1,(x._2,x._2.size)))

      println("dateUserInfo:" + dateUserInfo.count())

      val isActiveUser = userInfo.join(dateUserInfo).map(x =>(x._1,x._2._1._2 - x._2._2._2,x._2._1._1,x._2._2._1)).filter(_._2 > 0)
          .map(x => x._1 + "\t" + x._2 + "\t" + x._3.mkString(",") + "\t" + x._4.mkString(",")).collect()

      println("isActiveUser:" + isActiveUser.length)

        //.persist(StorageLevel.MEMORY_AND_DISK_2)

      FileUtil.writeToFile(args(1),isActiveUser)

      val schema =
        StructType(
          StructField("v_stock",StringType,nullable = false)::
          StructField("v_hour",StringType,nullable = false)::StructField("i_frequency",IntegerType,nullable = true)::Nil)

      // sqlContext.createDataFrame(dbData,schema).write.mode(SaveMode.Append).jdbc(MysqlOperation.connection,MysqlOperation.followTable,MysqlOperation.properties)

    } catch {
    case e:Exception =>
      PrismLogger.warn("follow write over")
    }finally {

    }
    sc.stop()
  }

}