package com.kunyan.wokongsvc.person

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.xml.XML

/**
  * Created by sijiansheng on 2016/10/20.
  */
object ReadHbaseData {

  def main(args: Array[String]) {

    val hbaseReadData = new ReadHbaseData("2181", "master,slave1,slave2,slave3,slave4")
    hbaseReadData.setTimeRange(1477612800000L, 1477618795000L)
    val hbaseConf = ConnectionFactory.createConnection(hbaseReadData.getConfiguration)
    val hTable = hbaseConf.getTable(TableName.valueOf("1"))
    val result = hTable.getScanner(hbaseReadData.getScan())
    val iteartor = result.iterator()
    val urls = new ListBuffer[String]()
    val contents = new ListBuffer[String]()

    while (iteartor.hasNext) {

      val cell = iteartor.next()
      val url = cell.getValue(Bytes.toBytes("basic"), Bytes.toBytes("url"))
      val content = cell.getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"))

      val newUrl = new String(url, "utf-8")
      val newContent = new String(content, "utf-8")
      urls += newUrl
      contents += newContent
      println(newUrl)

    }

    reduceByKey1(contents.toList).foreach(result => {
      println(result._1.substring(0, 10))
      println(result._2)
    })

  }

  def reduceByKey1(list: List[String]): mutable.Map[String, Int] = {

    val resultMap = mutable.Map[String, Int]()

    list.foreach(cell => {

      if (resultMap.contains(cell)) {
        val value = resultMap(cell) + 1
        resultMap.put(cell, value)
      } else {
        resultMap.put(cell, 1)
      }

    })

    resultMap
  }
}

class ReadHbaseData(val clientPort: String, val zookeeperQuorum: String) {

  val scan = new Scan()

  val configuration = {

    val configuration = HBaseConfiguration.create
    configuration.set("hbase.zookeeper.property.clientport", clientPort)
    configuration.set("hbase.zookeeper.quorum", zookeeperQuorum)

    configuration
  }


  def setTimeRange(startTime: Long, endTime: Long): Unit = {

    this.scan.setTimeRange(startTime, endTime)
    val scanToString = Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
  }

  def getScan(): Scan = {
    this.scan
  }

  def getConfiguration: Configuration = {
    this.configuration
  }

}
