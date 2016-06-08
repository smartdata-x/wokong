/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/TestHbase.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-05-20 09:47
#    Description  : 
=============================================================================*/
package com.kunyan.wokongsvc.realtimedata

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection,ConnectionFactory,HBaseAdmin,HTable,Put,Get,Result,Scan}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.api.java.function.Function
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext 

import scala.collection.mutable.ListBuffer

/**
  * Created by wukun on 2016/5/20
  * 自定义hbase相关的上下文类
  */
class HbaseContext(xml:XmlHandle) { self =>

  type Writable = ImmutableBytesWritable

  private val tableName = initTable
  private val colInfo = (xml.getElem("hbase","colfamilly"), xml.getElem("hbase", "colname"))
  private val conf = initConf
  private val scan = initScan

  private val sparkContext = new SparkContext(new SparkConf().setAppName("followHeat"))
  private val mysqlPool = MysqlPool(xml)

  /**
    * 获取所要操作的列族和列信息
    * @author wukun
    */
  def getColInfo:(String, String) = self.colInfo
 

  def initTable: String = xml.getElem("hbase", "tablename")

  /** 
    * 初始化操作hbase所涉及的Scan类
    * @author wukun
    */
  def initScan: Scan = {

    new Scan().addFamily(Bytes.toBytes(colInfo._1)).addColumn(
      Bytes.toBytes(colInfo._1), Bytes.toBytes(colInfo._2))
  }

  def changeScan(
    minStamp:Long, 
    maxStamp:Long
  ) {
    self.scan.setTimeRange(minStamp, maxStamp)
  }

  def initConf: Configuration = {

    val configuration = HBaseConfiguration.create
    val zookeeper = (xml.getElem("hbase", "clientport"),xml.getElem("hbase", "quorum"))
    configuration.set("hbase.zookeeper.property.clientPort", zookeeper._1)
    configuration.set("hbase.zookeeper.quorum", zookeeper._2)
    configuration.set(TableInputFormat.INPUT_TABLE, initTable)

    configuration
  }

  def changeConf {
    val scanToString = Base64.encodeBytes(ProtobufUtil.toScan(self.scan).toByteArray)
    self.conf.set(TableInputFormat.SCAN, scanToString)
  }

  def getConf: Configuration = self.conf

  /**
    * 产生hbase库数据RDD
    * @author wukun
    */
  def generateRDD:RDD[(Writable, Result)] = {
    sparkContext.newAPIHadoopRDD(self.conf, classOf[TableInputFormat], classOf[Writable], classOf[Result])
  }

  def broadcastPool: Broadcast[MysqlPool] = {
    val pool = sparkContext.broadcast(mysqlPool)

    pool
  }
}

/**
  * Created by wukun on 2016/5/20
  * HbaseContext伴生对象
  */
object HbaseContext {

  type ListTuple = ListBuffer[(String, Int)]

  /**
    * 完成hbase库中股票代码的解析
    * 定义成函数对象是为了避免整个类都要序列化
    */
  val dataHandle = new Function[String, ListTuple] {

    /**
      * @author youchaojiang
      */
    def call(value:String): ListTuple = {

      val followStockCodeList =  new ListTuple
      val pattern = "(?<=\")\\d{6}(?=\")".r
      val iterator = pattern.findAllMatchIn(value)

      while(iterator.hasNext) {
        val item = iterator.next
        followStockCodeList += ((item.toString, 1))
      }

      followStockCodeList
    }

    def apply(value:String): ListTuple = {
      call(value)
    }
  }

  def apply(xml:XmlHandle): HbaseContext = {
    new HbaseContext(xml)
  }
}

