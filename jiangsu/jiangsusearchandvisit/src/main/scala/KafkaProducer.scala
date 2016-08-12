import java.util.Properties

import com.kunyan.telecom.SUELogger
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

/**
  * Created by C.J.YOU on 2016/8/5.
  * kafka producer
  */
class KafkaProducer(topic: String) {

    private  val SEND_TOPIC = topic

    private  val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("metadata.broker.list", "192.168.110.101:9092,192.168.110.102:9092,192.168.110.103:9092,192.168.110.104:9092,192.168.110.105:9092,192.168.110.106:9092,192.168.110.107:9092,192.168.110.108:9092,192.168.110.109:9092,192.168.110.110:9092")
    props.put("request.required.acks","1")

    private  val producer = new Producer[String, String](new ProducerConfig(props))

    private def kafkaMessage(message: String): KeyedMessage[String, String] = {

        new KeyedMessage(SEND_TOPIC, null, message)

    }

    def send(message: String): Unit = {

        try {
            producer.send(kafkaMessage(message))
        } catch {
            case e: Exception =>
                SUELogger.exception(e)
        }
    }

}

object KafkaProducer {

    def apply(topic: String): KafkaProducer = new KafkaProducer(topic)
}