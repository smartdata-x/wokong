import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Created by C.J.YOU on 2016/8/5.
  * kafka conf
 */

object KafkaConf {

  val zkQuorum = "192.168.110.101:2182,192.168.110.102:2182,192.168.110.103:2182,192.168.110.104:2182,192.168.110.105:2182,192.168.110.106:2182,192.168.110.107:2182,192.168.110.108:2182,192.168.110.109:2182,192.168.110.110:2182"

   //val group = "Spark_"  //需要确保每个提交的job的kafka group名称不同
  val topics = "dpi_stream"
  val numThreads = 2

  def createStream(
                    ssc: StreamingContext,
                    zkQuorum: String,
                    groupId: String,
                    topics: Map[String, Int],
                    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER
                    ): ReceiverInputDStream[(String, String)] = {
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> groupId,
      "zookeeper.session.timeout.ms" -> "68000",
      "zookeeper.connection.timeout.ms" -> "105000",
      "zookeeper.sync.time.ms" -> "12000",
      "rebalance.max.retries"->"6",
      "rebalance.backoff.ms"->"9800",
      "auto.offset.reset" -> "largest")
    KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics, storageLevel)
  }

}
