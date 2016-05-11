package test

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

    /*val res = "eyJpZCI6IjQwIiwgInZhbHVlIjoiZUp3ek5EVFRNekt5MERNMEFXSUxRMDVERXpNRE15TmpNMU56QXhOanpvTE1aTDNjeXBMODhqeTl3a0s5NVB4Yy9ZeVNrb0tnMUdMOXN0U2k0c3o4UEwyU2loSk92M3lYMURSa0VnRFZqQm1YIn0=\neyJpZCI6Ijk4IiwgInZhbHVlIjoiZUp3Tno4dWlZekFBQU5DOVgrbUNvb2JGTE5UemtucFVTOWlwaUlTbWxNcWxYejl6L3VDRUZVK2toZXBmdjBwdks5c0luR1g5SHRQbVFWeVhYM0dqUWR3M0NiNlpKbkdJVVRSWmtlTnYzZHRJajhEMjB1dVI4M0Vwa3lpNG5vU01ycVcwZDFLbXYvRFRRTnFaMlZwS0hzcmcwN1FoalZ4M0ZjMEw0MnJmMlpFZyt2QnFzRzA4Zk1zdi9rYUIzS01hOXM0aFNCNTFZQXE4dHJXOW9QSkQvMW5sQkIxMGxEZHJhMk4zNDNpNzZvYXhtTVVjUHpNcW9UbWFLekJSNzdOVTZxVXJGdC92eXBzL3FjNWM0dDgvUmFRS1duUTlEc0hHUkl1cjhIWjI1bHoxNWJ0ck9jVjdHRE9Lck9jZS8wSU1uYmlxTW1WcXMybWt2VGhwNUhTOEt5cmJwUUI4Y0E2eVpHUm5BWVI3UVdJemY2THA0RG9xbHAra0pZYU92Mjhwb05HRlNSRXhSdW9Cd3pNS2pkaUxpODd0Rm1UZ215cXgwYktmR2l2cGFJOWYrV1YxQXV6bUMvUG9BTitsR0RhVkdJdWYveTFTdG14OXErWFZBejUvaEoxdFZPVHdTNGJsZkhWTDdXVkpCQnpUbFpIczFYUlQ3clRxV3FVK0VaQVhIdUtMbWJFVXhMM3JobmZiZlRhdUMrQU5lcEdXSE50TUY3dDJzYlNaZHBldkt0cys4MHJGSTg1d2V4Zk82U0dkRHEySWQycjEzaXJzNGhUOHBNM3B0TEFEV2J1eVFiRGtZRGx6MDg5Z3RHRm5ldzVsdG0vc004eXZwTUJxTzhIK3M2cnhaaTBRZWtZOGRVMnZtYWNvNFlIUVhEWUdXb3I2YkpFa2hmSGd5d0ZROEJTcW1nNkN0VVovYXNzblR0NkM1ZTgvckt6ZDRBPT0ifQ=="
    val json = StringUtil.parseJsonObject(res)
    println(json)*/

   /* val arr = sc.textFile("F:\\datatest\\telecom\\17_error").map(StringUtil.parseJsonObject).collect()
    FileUtil.writeToFile("F:\\datatest\\telecom\\17_correct",arr)*/


    /*sc.textFile("H:\\SmartData-X\\smartuser\\SmartUser_Eni\\上海\\stock_20160330").flatMap(_.split("\\.")).filter(x => x.length > 1 && !countryPrefix.contains(x)).distinct().foreach(x =>string = string +","+x)
  //println(string)
    FileUtil.writeStringToFile("H:\\SmartData-X\\smartuser\\SmartUser_Eni\\上海\\final_stock_20160330",string)*/

   /* val rdd1 = sc.textFile("H:\\SmartData-X\\smartuser\\SmartUser_Eni\\上海\\typeOne.txt").flatMap(_.split("\\."))
      .filter(x => x.length > 1 && !countryPrefix.contains(x)).distinct()

    val rdd3 = sc.textFile("H:\\SmartData-X\\smartuser\\SmartUser_Eni\\上海\\luxury_20160330").flatMap(_.split("\\."))
      .filter(x => x.length > 1 && !countryPrefix.contains(x)).distinct()

    val rdd2 = sc.textFile("H:\\SmartData-X\\smartuser\\SmartUser_Eni\\上海\\stock_20160330").flatMap(_.split("\\."))
      .filter(x => x.length > 1 && !countryPrefix.contains(x)).distinct()

    val arr = rdd1.++(rdd2).++(rdd3).zipWithIndex().map(x => x._2 +"\t"+x._1).collect()

    FileUtil.writeToFile("H:\\SmartData-X\\smartuser\\SmartUser_Eni\\上海\\final_index_url",arr)*/
    val data ="1461566996268\t81d031d5b2ae1aab3cdf28084d4cbef665417b28\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36\twww.ybzhan.cn\t/js/dialog_top.htm\thttp://www.ybzhan.cn/Product/Detail/275131.html\tASP.NET_SessionId=ak5b3w43u42vqfn0nokc3k3x; mtcached_mtsession_ak5b3w43u42vqfn0nokc3k3x=192.168.8.201:9720; Hm_lvt_fe76d250fc45bcdfc9267a4f6348f8d8=1461566984; Hm_lpvt_fe76d250fc45bcdfc9267a4f6348f8d8=1461566984\t中国"
    val en = TestStringUtil.zlibZip(data).replace("\r\n","")
    val vale ="{\"id\":\"327\", \"value\":\""+en+"\"}"
    println(vale)
   //   val arr = sc.textFile("E:\\workspace\\kafkadata\\sparktopic.txt").filter(_.split("\t").length ==12)
//        .map(DataAnalysis.extractorUrl).foreach(println)
    println(TestStringUtil.parseJsonObject(new BASE64Encoder().encode(vale.getBytes())))



  }

}
