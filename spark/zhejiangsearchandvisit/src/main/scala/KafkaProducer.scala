import java.util.Properties

import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

/**
  * Created by C.J.YOU on 2016/08/05.
  * kafka producer
  */
object KafkaProducer {

    val sendTopic = "dpi_kunyan"

    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("metadata.broker.list", "192.168.110.101:9092,192.168.110.102:9092,192.168.110.103:9092,192.168.110.104:9092,192.168.110.105:9092,192.168.110.106:9092,192.168.110.107:9092,192.168.110.108:9092,192.168.110.109:9092,192.168.110.110:9092")
    props.put("request.required.acks","1")

    val producer = new Producer[String, String](new ProducerConfig(props))

    def kafkaMessage(message: String): KeyedMessage[String, String] = {

        new KeyedMessage(sendTopic, null, message)

    }

    def send(message: String): Unit = {

        try {
            producer.send(kafkaMessage(message))
        } catch {
            case e: Exception => println(e)
        }
    }
}