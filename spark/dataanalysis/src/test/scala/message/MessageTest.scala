package message

import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by C.J.YOU on 2016/2/3.
  */
class MessageTest extends FlatSpec with Matchers{

  "send message " should "work " in{
    SendMessage.sendMessage(1,"电信平台计算", "异常短信测试")
  }
}
