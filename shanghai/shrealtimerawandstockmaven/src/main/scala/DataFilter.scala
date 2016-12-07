import com.kunyan.filter.Filter
import com.kunyan.util.FileUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by C.J.YOU on 2016/12/7.
  */
object DataFilter {


  val sparkConf = new SparkConf().setAppName("DataFilter")

  val sc = new SparkContext(sparkConf)

  def main(args: Array[String]) {

    val Array(dataDir, saveDir, fileName, stockCode) = args

    val data = Filter.filterStockCode(sc, dataDir, stockCode = stockCode)

    FileUtil.writeToFile(saveDir + "/" + fileName, data, isAppend = true )

  }

}
