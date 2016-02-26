import java.io.File

import com.kunyan.net.HotWordHttp

import scala.collection.mutable
import scala.io.Source

/**
  * Created by Administrator on 2016/1/28.
  */
object StringTest extends App {

  HotWordHttp.sendNew("http://120.55.189.211/cgi-bin/northsea/prsim/subscribe/1/hot_words_notice.fcgi", mutable.HashMap[String, String]("asdf" -> "asdf"))
}
