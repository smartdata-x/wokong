package pattern

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/9/6.
  */
object StockPattern {


  type Tuple2Map = (mutable.HashSet[String], (Map[String, String], Map[String, String], Map[String, String]))

  val DIGITPATTERN = "([0-9]{6})".r
  val ENCODEPATTERN = "((%.*){8})".r
  val ALPHAPATTERN = "([a-zA-Z]*)".r

  /**
    * 根据正则模式解析出不同格式的股票字符串
    *
    * @param stockStr 要解析的字符串
    * @author wukun
    */
  def stockCodeMatch(stockStr: String, alias: Tuple2Map): String = {

    stockStr match {

      case DIGITPATTERN(first) => if(alias._1(first)) first else "0"
      case ENCODEPATTERN(_*) => alias._2._1.getOrElse(stockStr, "0")
      case ALPHAPATTERN(first) => {
        val stock = first.toUpperCase
        alias._2._2.getOrElse(stock, alias._2._3.getOrElse(stock, "0"))
      }
      case _ => "0"

    }
  }


}
