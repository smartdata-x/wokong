package com.kunyan.data.scheduler

import java.util
import java.util.Properties

import com.kunyan.data.database.MysqlOperation
import com.kunyan.data.hbase.TableHbase
import com.kunyan.data.log.PrismLogger
import com.kunyan.data.util.{FileUtil, TimeUtil}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Created by C.J.YOU on 2016/5/27.
  * 获取hbase follow 数据的 主程序
  */
object FollowScheduler {

  var stockCodes:java.util.List[String] = new util.ArrayList[String]()

  def flatMapFun(timeStam:String,stockCodesTemp: mutable.HashSet[String]): mutable.MutableList[String] = {
    val lineList: mutable.MutableList[String] = mutable.MutableList [String]()
    val timeStamp = timeStam
    val hour = TimeUtil.getTime(timeStamp)
    /** follow */
    if (stockCodesTemp.nonEmpty  &&  timeStamp != null) {
      for (stockCode <- stockCodesTemp) {
        if(stockCodes.contains(stockCode)) {
          lineList += ("follow:" + stockCode + ":" + hour)
        }
      }
    }
    lineList
  }

  val sparkConf = new SparkConf()
    .setAppName("LoadData_Follow")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "2000")
    .setMaster("local")

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Usage: LoadData startDate endDate ")
      System.exit(-1)
    }

    MysqlOperation.getTableData("stock_info")

    try{

      TableHbase.getStockCodesFromHbase(sc,args(0),args(1))
      System.out.println("---Follow stockCodeMap size:----"+TableHbase.stockCodeMap.size)
      System.out.println("---Follow userMap size:----"+TableHbase.userMap.size)

      val data  = sc.parallelize(TableHbase.stockCodeMap.toSeq).flatMap(x =>flatMapFun(x._1,x._2)).map((_,1))
        .reduceByKey(_+_).cache()

      val result = data.map { line =>
          val arr = line._1.split(":")
          val stock = arr(1)
          val hour = arr(2)
        (stock +"\t"+ hour +"\t" + line._2,Row(stock,hour,line._2))
        }
      val dbData = result.map(_._2)
      val fileData = result.map(_._1)

      println("result size:"+result.count)

      val schema =
        StructType(
          StructField("v_stock",StringType,nullable = false)::
          StructField("v_hour",StringType,nullable = false)::StructField("i_frequency",IntegerType,nullable = true)::Nil)

      FileUtil.writeToFile(args(2),fileData.collect)

      // sqlContext.createDataFrame(dbData,schema).write.mode(SaveMode.Append).jdbc(MysqlOperation.connection,MysqlOperation.followTable,MysqlOperation.properties)

    } catch {
    case e:Exception =>
      PrismLogger.warn("follow write over")
    }finally {

    }
    sc.stop()
  }

}