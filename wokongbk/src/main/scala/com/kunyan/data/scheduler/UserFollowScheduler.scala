package com.kunyan.data.scheduler

import java.util

import com.kunyan.data.database.MysqlOperation
import com.kunyan.data.hbase.TableHbase
import com.kunyan.data.log.PrismLogger
import com.kunyan.data.util.{FileUtil, TimeUtil}
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * Created by C.J.YOU on 2016/5/27.
  * 获取hbase follow 数据的 主程序
  */
object UserFollowScheduler {

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

  val sparkConf = new SparkConf()
    .setAppName("UserFollowScheduler")
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
      System.err.println("Usage: LoadData startDate endDate ")
      System.exit(-1)
    }

    getTableData("stock_info")

    try{

      TableHbase.getStockCodesFromHbase(sc,args(0),args(1))
      System.out.println("---Follow stockCodeMap size:----"+TableHbase.stockCodeMap.size)
      System.out.println("---Follow userMap size:----"+TableHbase.userMap.size)

      val data  = sc.parallelize(TableHbase.userMap.toSeq).map(x =>mapFunction(x._1,x._2))
        //.persist(StorageLevel.MEMORY_AND_DISK_2)

      val fileData = data.collect()

      println("result size:"+ data.count)

      val schema =
        StructType(
          StructField("v_stock",StringType,nullable = false)::
          StructField("v_hour",StringType,nullable = false)::StructField("i_frequency",IntegerType,nullable = true)::Nil)

      FileUtil.writeToFile(args(2),fileData)

      // sqlContext.createDataFrame(dbData,schema).write.mode(SaveMode.Append).jdbc(MysqlOperation.connection,MysqlOperation.followTable,MysqlOperation.properties)

    } catch {
    case e:Exception =>
      PrismLogger.warn("follow write over")
    }finally {

    }
    sc.stop()
  }

}