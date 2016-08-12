import java.io.ByteArrayInputStream

import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory

/**
  * Created by C.J.YOU on 2016/8/5.
  * kafka数据使用avro模式解压
  * @param props 默认空值
  */
class AvroDecoder(props: VerifiableProperties = null) extends Decoder[String] {

  final val CDR_SCHEMA: Schema = Schema.parse("{\"type\":\"record\"," +
    "\"name\":\"CDR\"," +
    "\"fields\":[" +
    "{\"name\":\"ad\", \"type\":\"string\"}," +
    "{\"name\":\"ts\", \"type\":\"long\"}," +
    "{\"name\":\"host\", \"type\":\"string\"}," +
    "{\"name\":\"uri\", \"type\":\"string\"}," +
    "{\"name\":\"referrer\", \"type\":\"string\"}," +
    "{\"name\":\"srcip\", \"type\":\"string\"}," +
    "{\"name\":\"dstip\", \"type\":\"string\"}," +
    "{\"name\":\"ua\", \"type\":\"string\"}," +
    "{\"name\":\"cookie\", \"type\":\"string\"}," +
    "{\"name\":\"accept\", \"type\":\"string\"}" +
    "]}")

  final val READER: GenericDatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](CDR_SCHEMA)

  override def fromBytes(bytes: Array[Byte]): String = {

    val obj = READER.read(null, DecoderFactory.get.binaryDecoder(new ByteArrayInputStream(bytes), null))

    obj.get(0).toString + "\t" + obj.get(1).toString + "\t" + obj.get(2).toString + "\t" + obj.get(3).toString + "\t" + obj.get(4).toString + "\t" + obj.get(5).toString + "\t" + obj.get(6).toString + "\t" + obj.get(7).toString + "\t" + obj.get(8).toString + "\t" + obj.get(9).toString

  }
}

