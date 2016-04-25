import _root_.java.io.File
import _root_.java.io.PrintWriter
import java.io.{File, PrintWriter}

import _root_.org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lcm on 2016/4/21.
 * 统计url的访问次数
 */
object UrlCount {

  def main(args: Array[String]) {
    val in = args(0)
    val out = args(1)
    val sparkConf = new SparkConf().setAppName("FILTER AD")
    val sc = new SparkContext(sparkConf)

    sc.textFile(in).map(x => x.split("\t")).filter(_.length > 4)
      .map(x => {
        val url = x(3)
        if (url.startsWith("http") && url.split("/").length > 2) {
          (url.split("/")(2),1)
        } else {
          (url.split("/")(0),1)
        }
      })
      .reduceByKey(_ + _)
      .sortBy(_._2,ascending = false)
      .map(x => x._1 + "\t" + x._2)
      .saveAsTextFile(out)
  }
}