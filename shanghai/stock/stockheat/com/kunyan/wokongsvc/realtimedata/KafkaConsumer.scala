/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/KafkaConsumer.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-08-23 14:13
#    Description  : 
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import java.util.Properties
import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig
import kafka.consumer.ConsumerConnector
import kafka.consumer.ConsumerIterator
import kafka.consumer.KafkaStream
import kafka.consumer.ZookeeperConsumerConnector

import scala.collection._
import scala.None
import scala.Some
import scala.Option

/**
  * Created by wukun on 2016/08/19
  * kafka消费者操作句柄
  */
class KafkaConsumer extends CustomLogger {

  type ListStream = List[KafkaStream[Array[Byte],Array[Byte]]]

  private val topicMap = Map(
    KafkaConsumer.CONFIG("topic") -> KafkaConsumer.CONFIG("partition").toInt
  )
  private val config = initConfig
  private val connector = createConnector
  private val stream = createStream

  def initConfig: ConsumerConfig = {

    val props = new Properties 

    props.put("zookeeper.connect", KafkaConsumer.CONFIG("zkConnect"))
    props.put("socket.timeout.ms", KafkaConsumer.CONFIG("sockTimeout"))
    props.put("group.id", KafkaConsumer.CONFIG("group"))
    val consumerConfig = new ConsumerConfig(props)

    consumerConfig
  }

  def createConnector: ConsumerConnector = {
    val consumerConnector = Consumer.create(config)
    consumerConnector
  }

  def createStream: Map[String, ListStream] = {
    connector.createMessageStreams(topicMap)
  }

  def getSpecifyStream(topic: String): Option[ListStream] = {

    try {
      val ret = stream(topic).asInstanceOf[ListStream]
      Some(ret)
    } catch {
      case e: Exception => {
        None
      }
    }

  }

  def partitionTotal: Int = {

    var num = 0
    topicMap.foreach( x => {
      num += x._2
    })

    num
  }

  def getStreams: Map[String, ListStream] = {
    stream
  }

  def shutdown {
    connector.shutdown
  }
}

/**
  * Created by wukun on 2016/08/19
  * kafka消费者操作句柄伴生对象
  */
object KafkaConsumer extends CustomLogger {

  val CONFIG = {

    val xmlHandle = XmlHandle.getInstance

    Map(
      "zkConnect"   -> xmlHandle.getElem("kafkaconsumer", "zookeeper"),
      "sockTimeout" -> xmlHandle.getElem("kafkaconsumer", "sockTimeout"),
      "topic"       -> xmlHandle.getElem("kafkaconsumer", "topic"),
      "partition"   -> xmlHandle.getElem("kafkaconsumer", "partition"),
      "group"       -> xmlHandle.getElem("kafkaconsumer", "group")
    )
  }

  def main(args: Array[String]) {
    try {
      val useConsumer = new KafkaConsumer
      useConsumer.getSpecifyStream("month_heat") match {
        case Some(listStream) => {
          val iter = listStream(0).iterator
          while(iter.hasNext) {
            println(new String(iter.next.message))
          }
        }
        case None => {
          System.exit(-1)
        }
      }
    } catch {
      case e: Exception => {
        println(e.getMessage)
      }
    }
  }
}

