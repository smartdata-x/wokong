package com.kunyan.data.scheduler

import com.kunyan.data.util.FileUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by C.J.YOU on 2016/6/1.
  */
object ChooseActiveUser {

  val sparkConf = new SparkConf()
    .setAppName("ChooseActiveUser")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "2000")
    .setMaster("local")

  val sc = new SparkContext(sparkConf)

  def keyValueMap(array: Array[String]): (String, String) = {
    val user = array(0)
    val weight = array(1)
    (user, weight)
  }

  def getMaxSize(one: String, two: String): String = {

    if(one.toInt > two.toInt) one else two

  }

  def main(args: Array[String]) {

    if(args.length < 3)
      sys.error("Usage: dataFilePath, topUserNum, saveFilePath")

    val Array(dataFile, topUser, saveFile) = args

    val activeUserData = sc.textFile(dataFile)
      .map(_.split("\t")).map(keyValueMap).distinct()
      .reduceByKey(getMaxSize).map(x =>(x._2,x._1)).sortByKey(ascending = false).top(topUser.toInt)

    FileUtil.writeUserFollowToFile(saveFile,activeUserData,0)
    FileUtil.writeUserFollowToFile(saveFile +"_userid",activeUserData,1)

  }

}
