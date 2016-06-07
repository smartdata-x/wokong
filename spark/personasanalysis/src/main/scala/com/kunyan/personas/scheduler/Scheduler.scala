package com.kunyan.personas.scheduler

import java.util.Properties
import com.kunyan.personas.config.SparkConfig
import com.kunyan.personas.database.{DBOperation, Table}
import com.kunyan.personas.extractor.Extractor
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

object Scheduler extends Serializable{

  val sparkConf = new SparkConf()
    // .setMaster("local")
    .setAppName("USER")
    .set("spark.serializer",SparkConfig.SPARK_SERIALIZER)
    .set("spark.kryoserializer.buffer.max",SparkConfig.SPARK_KRYOSERIALIZER_BUFFER_MAX)
    .set("spark.driver.allowMultipleContexts","true")

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  var distinctPhone = new HashSet[String]
  var distinctQQ = new HashSet[String]
  var distinctWeibo = new HashSet[String]

  def main(args: Array[String]): Unit = {
    // 策略二 （离线）
    try {
      val data = sc.textFile(args(0)).distinct()
      println("start extract user info:")
      val userInfo  = data.map(_.split("\t"))
        .filter(_.length == 8)
        .filter(x => x(6) != "NoDef")
        .filter(x => x(3).contains("weibo.com") || x(3).contains("suning.com") || x(3).contains("qq.com") || x(3).contains("dianping.com") || x(3).contains("4008823823.com.cn"))
        .map(Extractor.extratorUserInfo)
        .filter(filterUserInfo)
        .distinct().persist(StorageLevel.MEMORY_AND_DISK).repartition(3)

      val updateUserInfo = userInfo.map(divideOptimizerMap).groupByKey()
      val item0 = updateUserInfo.lookup(0)
      if(item0.nonEmpty){
        val insertUserInfo = item0.head.filter(filterUserInfo)
        println("insertIterator size:"+insertUserInfo.size)
        println("INSERTING.........................")
        DBOperation.batchInsert(Array("phone", "qq", "weibo"),insertUserInfo)
        println("INSERTED.........................")
      }
      val item1 = updateUserInfo.lookup(1)
      val item2 = updateUserInfo.lookup(2)
      val item3 = updateUserInfo.lookup(3)
      println("UPDATING...........................")
      if(item1.nonEmpty){
        val phoneUpdateIterator = item1.head
        println("phoneUpdateIterator size:"+phoneUpdateIterator.size)
        DBOperation.batchUpdate(Array("qq", "weibo"),phoneUpdateIterator)
      }
      if(item2.nonEmpty){
        val qqUpdateIterator = item2.head
        println("qqUpdateIterator size:"+qqUpdateIterator.size)
        DBOperation.batchUpdate(Array("phone", "weibo"),qqUpdateIterator)
      }
      if(item3.nonEmpty) {
        val weiboUpateIterator = item3.head
        println ("weiboUpateIterator size:" + weiboUpateIterator.size)
        DBOperation.batchUpdate (Array ("phone", "qq"), weiboUpateIterator)
      }
      println("updated mysql main_index:")
    } catch {
      case e: Exception =>
      println(e.getMessage)
    } finally {
        DBOperation.connection.close()
    }
  println("end")

  }
  def filterUserInfo(x:(String,String,String)):Boolean ={
    if(x._1.nonEmpty || x._2.nonEmpty || x._3.nonEmpty) true else false
  }
  // insert
  def divideInsertMap(item:(String,String,String)) : (Int,(String,String,String)) = {
    var phone = item._1
    var qq = item._2
    var weibo = item._3
    if (phone.isEmpty) phone = "Nodef"
    if (qq.isEmpty) qq = "Nodef"
    if (weibo.isEmpty) weibo = "Nodef"
    val isDistinctPhone = distinctPhone.contains(phone)
    val isDistinctQQ = distinctQQ.contains(qq)
    val isDistinctWeibo = distinctWeibo.contains(weibo)

    val phoneDataFrame = Table.isExist("phone",phone,DBOperation.connection)
    if(phoneDataFrame._1 != -1){
      val id = phoneDataFrame._1
      if (qq == "Nodef")
        qq = phoneDataFrame._3
      if (weibo == "Nodef")
        weibo = phoneDataFrame._4
      (1,(qq, weibo, id.toString))
    } else {
      val qqDataFrame = Table.isExist("qq",qq,DBOperation.connection)
      if(qqDataFrame._1 != -1){
        val id = qqDataFrame._1
        if (phone == "Nodef")
          phone = qqDataFrame._2
        if (weibo == "Nodef")
          weibo = qqDataFrame._4
        (2,(phone, weibo, id.toString))
      } else {
        val weiboDataFrame = Table.isExist("weibo",weibo,DBOperation.connection)
        if(weiboDataFrame._1 != -1){
          val id = weiboDataFrame._1
          if (phone == "Nodef")
            phone = weiboDataFrame._2
          if (qq == "Nodef")
            qq = weiboDataFrame._3
          (3,(phone, qq, id.toString))
        } else {
          if(!isDistinctPhone && !isDistinctQQ && !isDistinctWeibo){
            if(phone != "Nodef")
              distinctPhone = distinctPhone.+(phone)
            if(qq != "Nodef")
              distinctQQ = distinctQQ.+(qq)
            if(weibo != "Nodef")
              distinctWeibo = distinctWeibo.+(weibo)
            (0,(item._1, item._2, item._3))
          } else (0,("","",""))
        }
      }
    }
  }
  // Optimizer
  def divideOptimizerMap(item:(String,String,String)) : ((Int,(String,String,String))) = {

    var phone = item._1
    var qq = item._2
    var weibo = item._3
    if (phone.isEmpty) phone = "Nodef"
    if (qq.isEmpty) qq = "Nodef"
    if (weibo.isEmpty) weibo = "Nodef"

    // println(phone+","+qq+","+weibo)

    val isDistinctPhone = distinctPhone.contains(phone)
    val isDistinctQQ = distinctQQ.contains(qq)
    val isDistinctWeibo = distinctWeibo.contains(weibo)

    val dbData = Table.getInstance(sqlContext).value

    // println("search ->>>>>>>>>>>>>>>")
    val phoneArr = dbData.filter(dbData("phone") === phone).take(1)
    if(phoneArr.nonEmpty){
      // println("phoneArr in")
      val phoneRow = phoneArr(0)
      val phoneDataFrame = (phoneRow(0),phoneRow(1).toString,phoneRow(2).toString,phoneRow(3).toString)
      val id = phoneDataFrame._1
      if (qq == "Nodef")
        qq = phoneDataFrame._3
      if (weibo == "Nodef")
        weibo = phoneDataFrame._4
      (1,(qq, weibo, id.toString))
    } else {
      val qqArr = dbData.filter(dbData("qq") === qq).take(1)
      if(qqArr.nonEmpty){
        // println("qqArr in")
        val qqRow = qqArr(0)
        // println(qqRow(0)+","+qqRow(1).toString+","+qqRow(2).toString+","+qqRow(3).toString)
        val qqDataFrame = (qqRow(0),qqRow(1).toString,qqRow(2).toString,qqRow(3).toString)
        val id = qqDataFrame._1
        if (phone == "Nodef")
          phone = qqDataFrame._2
        if (weibo == "Nodef")
          weibo = qqDataFrame._4
        // println(phone+","+weibo+","+id.toString)
        (2,(phone, weibo, id.toString))
      } else {
        val weiboArr = dbData.filter(dbData("weibo") === weibo).take(1)
        if(weiboArr.nonEmpty){
          // println("weiboArr in")
          val weiboRow = weiboArr(0)
          val weiboDataFrame = (weiboRow(0),weiboRow(1).toString,weiboRow(2).toString,weiboRow(3).toString)
          val id = weiboDataFrame._1
          if (phone == "Nodef")
            phone = weiboDataFrame._2
          if (qq == "Nodef")
            qq = weiboDataFrame._3
          (3,(phone, qq, id.toString))
        } else {
          if(!isDistinctPhone && !isDistinctQQ && !isDistinctWeibo){
            // println("isDistinct in")
            if(phone != "Nodef")
              distinctPhone = distinctPhone.+(phone)
            if(qq != "Nodef")
              distinctQQ = distinctQQ.+(qq)
            if(weibo != "Nodef")
              distinctWeibo = distinctWeibo.+(weibo)
            (0,(item._1, item._2, item._3))
          } else {
            println("isNotDistinct in")
            (0,("","",""))
          }
        }
      }
    }
  }
}
