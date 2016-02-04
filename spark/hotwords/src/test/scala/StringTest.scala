import java.io.File

import scala.io.Source

/**
  * Created by Administrator on 2016/1/28.
  */
object StringTest extends App {

  val filestockk = new File("F:/stock.txt")
  Source.fromFile(filestockk, "UTF-8").getLines().foreach(keyValuestock => {
    val arr = keyValuestock.split("\t")
    val r1= arr(0)
    val name = arr(1)
    val code =r1.split("\\.")(0)
    println(code)
    println(name)

    //          val keystock =r1(0).split(".")
    //          alert(keystock[0])
    //          val valuestock =r1(1)
    //          mapstockk+=(valuestock->keystock)
  })
}
