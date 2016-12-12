/*=============================================================================
# Copyright (c) 2015
# ShanghaiKunyan.  All rights reserved
# Filename : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/KafkaProducer.scala
# Author   : Sunsolo
# Email    : wukun@kunyan-inc.com
# Date     : 2016-08-19 10:07
=============================================================================*/

package com.kunyan.wokongsvc.realtimedata

import java.util.Properties

import com.kunyan.wokongsvc.realtimedata.JsonHandle.{MixData, StockInfo}
import kafka.common.FailedToSendMessageException
import kafka.producer.{KeyedMessage, Producer, ProducerClosedException, ProducerConfig}

import scala.collection.mutable.ListBuffer

/**
  * Created by wukun on 2016/08/19
  * kafka生产者操作句柄
  */
class KafkaProducer(val xmlHandle: XmlHandle)
  extends Serializable {

  private lazy val topic = KafkaProducer.CONFIG("topic")
  private lazy val config = initConfig
  private var producer = initProducer
  private var reconncount = 0

  def initConfig: ProducerConfig = {

    val props = new Properties

    props.put("metadata.broker.list", KafkaProducer.CONFIG("broker"))
    props.put("serializer.class", KafkaProducer.CONFIG("serializer"))

    /** 发送失败后的重试次数，要配合请求延迟一块使用 */
    props.put("message.send.max.retries", KafkaProducer.CONFIG("retries"))

    /** 在发送消息后等待反馈的时间，这个也是底层进行socket连接时等待连接的延迟时间，配合重试次数使用 */
    props.put("request.timeout.ms", KafkaProducer.CONFIG("requesttimeout"))

    /** 发送失败后，等待下次发送的时间间隔 */
    props.put("retry.backoff.ms", KafkaProducer.CONFIG("retrybackoff"))
    props.put("partitioner.class", "com.kunyan.wokongsvc.realtimedata.CustomPartitioner")

    new ProducerConfig(props)
  }

  def initProducer: Producer[String, String] = {
    val tmpPro = new Producer[String, String](config)
    tmpPro
  }

  def reconnect {
    producer.close
    producer = new Producer[String, String](config)
  }

  /**
    * 默认的producer发送消息接口
    *
    * @param  message 要发送的消息
    */
  def send(key: String, message: String) {
    val sendMsg = KafkaProducer.constructKeyMessage((this.topic, key, message))
    send(sendMsg)
  }

  /**
    * producer发送消息接口
    *
    * @param  message 要发送的消息
    */
  def send(varTopic: String, key: String, message: String) {
    val sendMsg = KafkaProducer.constructKeyMessage((varTopic, key, message))
    send(sendMsg)
  }

  /**
    * producer发送消息接口
    * 注意重发逻辑以及底层重连机制是kafka本身提供的，这里捕获的异常
    * 一定要分清
    */
  def send(keyMessage: ListBuffer[KeyedMessage[String, String]]) {

    while (reconncount <= 1) {

      try {
        producer.send(keyMessage: _*)
        reconncount = 2
      } catch {

        /** 这个地方代表producer的shutdown标志位被置为true */
        case e: ProducerClosedException => {

         exception(e)
          reconncount = reconncount + 1
          if (reconncount <= 1) {
            Thread.sleep(1000)
            reconnect
          }
        }

        /** 这个异常是底层重试设置的次数以后才报的异常 */
        case e: FailedToSendMessageException => {
         exception(e)
        }

        /** 这个异常底层并没抛出，但为了严谨以及查找错误，所以进行捕获,
          * 另一方面也是为了防止程序轻易的退出
          */
        case e: Exception => {
         exception(e)
        }
      }

    }

    reconncount = 0
  }

}

/**
  * Created by wukun on 2016/08/19
  * kafka生产者句柄伴生对象
  */
object KafkaProducer {

  val CONFIG = {

    val xmlHandle = XmlHandle.getInstance

    Map(
      "broker" -> xmlHandle.getElem("kafka", "broker"),
      "serializer" -> xmlHandle.getElem("kafka", "serializer"),
      "topic" -> xmlHandle.getElem("kafka", "webtopic"),
      "retries" -> xmlHandle.getElem("kafka", "retries"),
      "requesttimeout" -> xmlHandle.getElem("kafka", "requesttimeout"),
      "retrybackoff" -> xmlHandle.getElem("kafka", "retrybackoff"),
      "producerreconn" -> xmlHandle.getElem("kafka", "producerreconn"),
      "producertimeout" -> xmlHandle.getElem("kafka", "producertimeout")
    )
  }

  def apply(xml: XmlHandle): KafkaProducer = {
    new KafkaProducer(xml)
  }

  def constructKeyMessage(elems: (String, String, String)*): ListBuffer[KeyedMessage[String, String]] = {

    /** 因为实例化ListBuffer时变参初始化，所以一定要加上括号 */
    val listMessage = ListBuffer[KeyedMessage[String, String]]()
    elems.foreach(x => {
      listMessage += new KeyedMessage[String, String](x._1, x._2, x._3)
    })

    listMessage
  }

  def packMessageParam(
                        topic: String,
                        key: String,
                        stamp: Long,
                        month: Int,
                        day: Int,
                        hour: Int,
                        stockInfo: List[StockInfo],
                        deno: Int,
                        mole: Int
                      ): (String, String, String) = {

    val size = stockInfo.length
    import com.kunyan.wokongsvc.realtimedata.MixTool._
    val value = JsonHandle.toString(MixData("", stamp, month, day, hour, stockInfo.slice(division(deno - 1, mole, size), division(deno, mole, size)))
    )

    (topic, key, value)

  }
}
