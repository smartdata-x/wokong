package test

import java.net.URLDecoder

import config.FileConfig
import org.apache.spark.{SparkContext, SparkConf}
import sun.misc.BASE64Encoder
import util.{FileUtil, StringUtil}

/**
  * Created by C.J.YOU on 2016/3/17.
  */
object Test {

  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("USER")

  val sc = new SparkContext(sparkConf)
  var string = ""
  private  val countryPrefix = ",es,jp,nl,pt,br,uk,se,fr,de,kr,cz,gr,id,no,hu,ru,in,fi,ro,dk,com,cn,gov,net,hk,tw,www,"
  def main(args: Array[String]) {

   /* val rdd1 = sc.textFile("H:\\SmartData-X\\smartuser\\SmartUser_Eni\\上海\\typeOne.txt").flatMap(_.split("\\."))
      .filter(x => x.length > 1 && !countryPrefix.contains(x)).distinct()

    val rdd3 = sc.textFile("H:\\SmartData-X\\smartuser\\SmartUser_Eni\\上海\\luxury_20160330").flatMap(_.split("\\."))
      .filter(x => x.length > 1 && !countryPrefix.contains(x)).distinct()

    val rdd2 = sc.textFile("H:\\SmartData-X\\smartuser\\SmartUser_Eni\\上海\\stock_20160330").flatMap(_.split("\\."))
      .filter(x => x.length > 1 && !countryPrefix.contains(x)).distinct()

    val arr = rdd1.++(rdd2).++(rdd3).zipWithIndex().map(x => x._2 +"\t"+x._1).collect()

    FileUtil.writeToFile("H:\\SmartData-X\\smartuser\\SmartUser_Eni\\上海\\final_index_url",arr)*/

  /*  var str ="eyJpZCI6IjM5OCIsICJ2YWx1ZSI6ImVKeU5rRjF2bXpBVWhxLzVGNzRwYXFYSjJNZkd4cXFzQ1RBa1ZVZFNRZFpjVmlSeEZsU0Nzd1NwMDM3OVRMOXVPMWs2MysvUmM4d0ItPFV4cGp5aVNXUEtCYzBFUkp3aGhJRVd3SHZPbUdYM2pyamtGMHNlMTVlL2orVzE4QkhOckxGWkNOdFlOM1IzdmMyTE1QM042YjhkQk4tPHZVczNXdS8yempmQUI5NDJiaXEwNTljczlmWnRpMTg3Mk4xbjhUUk94WGJZZlF4QXVPLzZVYmQ5SDViTHV0SVBSVjBFQzJmc1BxamMtPDM2N3YyeWpHQkYydnUySG5YaTVvc1VJQzAxdTBYcTRGdjBIcDZkVGJ0ZDNjZDJNVSt5T1pRTmYzODFYMTR4dnF1MmVMWm5iNzdHNVEtPDFmNFpEMjZJT0o2ZUlBVGxoN003Mm9nUlREQ05sY0tVVU5TMC9vRHVmVkhRTE9yNThrR3ZHaTFZckNSWExHRlNNdUxiTXJ6VEh1S3AtPE1MUGlxZEdscG1Gelp6UVRtUktaTUprQUxsU1prcEpKQVNMTFpCS0xKSkVmaXNkSlhQMzBDZ3JNNUlRbVVCb2h3REFvWlo0cldrQWgtPHBXQWNibEZUNTNPajAvTDFaOTdTU2Zpb0laeE5RYzQ4WEp4SUJUbm5zYUVLR0M5Wm1nTUZRenpCdTZTcHRWbG1HZ2dWaElQeUpFMmotPC80OTVBczIrSlAwSGIyMmdBdz09IiwgImtleSI6IiJoYXMgYmVlbiBtZW1iZXIgb2YgdGhpcyBzaXRlIGZvciIgIlNvIGZhciAqIGhhcyBlYXJuZWQgKiBwdHMgYW5kIGhhcyIifQ=="

    println(TestStringUtil.parseJsonObject(str))*/

    // 匹配url的过滤
   /* val rdd1 = sc.textFile("H:\\SmartData-X\\smartuser\\SmartUser_Eni\\上海\\采集url格式\\2016-04-29-新增匹配url").filter(_.split("\t").length > 1).map(_.split("\t")(1))

    val rdd3 = sc.textFile("H:\\SmartData-X\\smartuser\\SmartUser_Eni\\上海\\采集url格式\\index_url_20160427").filter(_.split("\t").length > 1).map(_.split("\t")(1))

    val arr = rdd1.++(rdd3).distinct().zipWithIndex().map(x => x._2 +"\t"+x._1).collect()
    FileUtil.writeToFile("H:\\SmartData-X\\smartuser\\SmartUser_Eni\\上海\\采集url格式\\index_url_20160429",arr)*/

    val key ="Pz8/Pz8/Pz8/Pz8/"
    val v = TestStringUtil.decodeBase64(key)
    println(v)
  }

}
