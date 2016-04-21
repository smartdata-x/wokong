import java.io.{File, PrintWriter}

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lcm on 2016/4/21.
 * 统计url的访问次数
 */
object UrlCount {

  val url_count = new scala.collection.mutable.ListBuffer[String]()

  def main(args: Array[String]) {
    val in = args(0)
    val out = args(1)
    val sparkConf = new SparkConf().setAppName("FILTER AD")
    val sc = new SparkContext(sparkConf)

    val writerUrlCount = new PrintWriter(new File(out), "UTF-8")

    sc.textFile(in).map(x => x.split("\t")).filter(_.length > 4)
      .filter(_ (3).contains("/"))
      .map(x => {
        val url = x(3)
        if (url.contains("http://") && url.split("/").length > 3) {
          (url.split("/")(2), 1)
        } else {
          (url.split("/")(0), 1)
        }
      })
      .reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2 + "\t" + x._1))
      .foreach(x => {
        url_count.+=(x)
      })
    for (x <- url_count) {
      writerUrlCount.write(x + "\n")
    }
    writerUrlCount.close()
  }
}
