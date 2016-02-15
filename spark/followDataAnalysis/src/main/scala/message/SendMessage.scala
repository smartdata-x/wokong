package message

import com.kunyan.scalautil.message.TextSender
import config.MessageConfig

/**
  * Created by C.J.YOU on 2016/2/3.
  * 发送异常短信消息接口
  */
object SendMessage {

  def sendMessage(mobileGroup:Int,key:String,msg:String): Unit ={
    if(mobileGroup == 1){
      TextSender.send(MessageConfig.userid,MessageConfig.account,MessageConfig.password,MessageConfig.calculateMobileGroup,key,msg)
    }else if(mobileGroup == 2){
      TextSender.send(MessageConfig.userid,MessageConfig.account,MessageConfig.password,MessageConfig.hbaseMobileGroup,key,msg)
    }

  }

}
