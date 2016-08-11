package hbase

import config.HbaseConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.Base64

/**
  * Created by C.J.YOU on 2016/1/13.
  * Hbase 操作子类的实现，用于操作hbase具体表中的信息
  * connect hbase get info
  * please ensure HBASE_CONF_DIR is on classpath of spark driver
  * e.g: set it through spark.driver.extraClassPath property
  * in spark-defaults.conf or through --driver-class-path
  * command line option of spark-submit
  */
class HBase {

  private[this] var _tableName: String = new Predef.String

  def tableName: String = _tableName

  def tableName_=(value: String): Unit = {
    _tableName = value
  }

  private[this] var _columnFamliy: String = new Predef.String

  def columnFamliy: String = _columnFamliy

  def columnFamliy_=(value: String): Unit = {
    _columnFamliy = value
  }

  private[this] var _column: String = new Predef.String

  def column: String = _column

  def column_=(value: String): Unit = {
    _column = value
  }

  private var conf = new Configuration()

  /** 设置自定义扫描hbase的scan */
  def setScan(scan:Scan): Unit ={
    val proto:ClientProtos.Scan = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)
    conf.set(TableInputFormat.SCAN, scanToString)
  }

  /** 获取hbase的配置  */
  def getConfigure(table:String,columnFamliy:String,column:String): Configuration = {
    conf = HBaseConfiguration.create()
    conf.set("hbase.rootdir", HbaseConfig.HBASE_ROOT_DIR)
    //使用时必须添加这个，否则无法定位
    conf.set("hbase.zookeeper.quorum", HbaseConfig.HBASE_ZOOKEEPER_QUORUM)
    conf.set("hbase.zookeeper.property.clientPort", HbaseConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
    conf.set(TableInputFormat.INPUT_TABLE, table)
    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY,columnFamliy)
    conf.set(TableInputFormat.SCAN_COLUMNS,column)
    conf
  }
}
