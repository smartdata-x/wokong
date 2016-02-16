package hbase
import java.util.regex.Pattern

import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/1/15.
  * Hbase 操作子类，用来操作Hbase中的表1
  */
object TableHbase extends HBase {

  val ONE_TABLE = "1"
  val ONE_COLUMN_FAMILY = "basic"
  val ONE_COLUMN_MEMBER = "content"
  var userMap = new mutable.HashMap[String, ListBuffer[String]]
  var stockCodeMap = new mutable.HashMap[String, Set[String]]
  val oneTable = new HBase
  oneTable.tableName = ONE_TABLE
  oneTable.columnFamliy = ONE_COLUMN_FAMILY
  oneTable.column = ONE_COLUMN_MEMBER

  def getUserMap: mutable.HashMap[String, ListBuffer[String]] ={
    this.userMap
  }

  def getStockCodeMap:mutable.HashMap[String, Set[String]] = {
    this.stockCodeMap
  }

  /** 按照rowKey 将获取hbase的数据 */
  def get(rowKey:String,table:String,columnFamliy:String,column:String):Result = {
    val connection = ConnectionFactory.createConnection(HBaseConfiguration.create())
    val htable = connection.getTable(TableName.valueOf("1"))
    val get = new Get(Bytes.toBytes(rowKey))
    val resultOfGet = htable.get(get)
    resultOfGet
  }

  /**获取股票代码 */
  def getStockCodes(response: String):  ListBuffer[String] ={
    val followStockCodeList =  new ListBuffer[String]
    val pattern = "(?<=\")\\d{6}(?=\")".r
    val iterator = pattern.findAllMatchIn(response)

    while(iterator.hasNext) {
      val item = iterator.next
      followStockCodeList.+=(item.toString)
    }

    followStockCodeList
  }

  /** 获取Userid */
  def getUserId(sc:String):String = {
    var userId = new String
    val pattern = Pattern.compile("\\[\'userid\'\\]\\s*=\\s*\'\\d{1,}\'")
    val m = pattern.matcher(sc)
    if(m != null){
      if(m.find()) {
        val outputValue = m.group(0)
        if (outputValue != null) {
          val patternId = Pattern.compile("\\d{1,}")
          val mId = patternId.matcher(outputValue)
          if(mId.find()) {
            val outputValueId = mId.group(0)
            userId = outputValueId
          }
        }
      }
    }
    userId
  }

  /** 使用spark运行获取Hbase股票信息 */
  def getStockCodesFromHbase(sc:SparkContext,timeRange:String): Array[String] = {
    /** get hbase data */
    val scan = new Scan()
    val currentTimeStamp = System.currentTimeMillis()
    println("getStockCodesFromHbase ------ timeRange:"+timeRange+":")
    scan.setTimeRange(timeRange.toLong, currentTimeStamp)
    val conf = oneTable.getConfigure(oneTable.tableName, oneTable.columnFamliy, oneTable.column)

    oneTable.setScan(scan)

    val users = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val stockCodes = users.flatMap(x => {
      try {
        val result = x._2
        val value = Bytes.toString(result.getValue(Bytes.toBytes(oneTable.columnFamliy), Bytes.toBytes(oneTable.column)))
        val timeStamps = result.getColumnLatestCell(Bytes.toBytes(oneTable.columnFamliy), Bytes.toBytes(oneTable.column)).getTimestamp.toString
        val userId = getUserId(value)
        val userStockList = getStockCodes(value)
        TableHbase.userMap.put(userId, userStockList)
        TableHbase.stockCodeMap.put(timeStamps,userStockList.toSet)
        userStockList

      } catch {
        case e: Exception => println("[C.J.YOU] getStockCodesFromHbase error")
          ListBuffer[String]()
      }
    }).collect()
    println("hbase get data:<<<<<<<<<<<<<<<<<<<<<<<<"+stockCodes.length)
    stockCodes
  }

  /** 合并List集合：主要要用于去重股票代码 */
  def mergeList(global_list: mutable.MutableList[String],temp_list:mutable.MutableList[String]): mutable.MutableList[String] ={
    if(temp_list != null) {
      val iterator = temp_list.iterator
      while (iterator.hasNext) {
        val value = iterator.next()
        if (!global_list.contains(value)) {
          global_list.+=(value)
        }
      }
    }
    global_list
  }
}
