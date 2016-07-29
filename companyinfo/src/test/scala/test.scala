import com.kumyan.companyinfo.parser.CpnyExecutives
import org.jsoup.Jsoup
import org.jsoup.select.Elements

/**
  * Created by niujiaojiao on 2016/7/22.
  */
object test extends App {

  val doc = Jsoup.connect("http://f10.eastmoney.com/f10_v2/ShareholderResearch.aspx?code=sh601020").get()

  //十大流通股东

  var tableTop = new Elements()

  if (doc.toString.contains("TTCS_Table_Div")) {

    tableTop = doc.getElementById("TTCS_Table_Div")
      .select("table tbody")

  } else {

    tableTop = null
  }

  if(tableTop != null){

    var partOneDates = new Elements()

    if (doc.toString.contains("sdltgd")) {

      partOneDates = doc.getElementById("sdltgd").nextElementSibling().getElementsByTag("li")

    } else {
      partOneDates = null
    }

    if (tableTop != null && partOneDates != null) {
      if (tableTop.size != partOneDates.size) {
        println(partOneDates.size)
        println(tableTop.size)
        println(tableTop)
        println(partOneDates)
      }
    }

  }



}
