package test

import org.apache.spark.{SparkConf, SparkContext}
import util.FileUtil

/**
  * Created by C.J.YOU on 2016/8/30.
  * 江苏电信实时stock统计类
  */
object JsdxCount {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("JsdxCount"))

    val Array(dataDir, date, saveDir) = args

    val source = sc.textFile(dataDir + "/" + date).map(_.split("\t")).filter(_.length == 3).map{ x =>

      val flag = x(2).toInt
      val key:String = if(flag >=0  && flag <= 41) "search" else if(flag >=42  && flag <= 95) "visit" else ""
      (key,1)

    }.reduceByKey(_ + _).map(x => x._1 + "\t" + x._2).collect()


    val savePath = saveDir  +"/" + date

    FileUtil.mkDir(savePath)

    FileUtil.write(savePath + "/" + "count_"+ date ,source)


  }

}
