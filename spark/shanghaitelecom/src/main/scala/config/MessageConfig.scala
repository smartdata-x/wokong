package config

/**
  * Created by C.J.YOU on 2016/8/2.
  */
object MessageConfig {

  val config = XMLConfig.confFile

  val RECEIVER = (config \ "Message" \ "receiver").text
  val KEY = (config \ "Message" \ "key").text
  val MESSAGE_CONTEXT = (config \ "Message" \ "context").text

}
