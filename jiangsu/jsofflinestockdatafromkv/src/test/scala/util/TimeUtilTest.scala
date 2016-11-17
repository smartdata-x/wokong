package util

import java.util.{Calendar, Date}

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by C.J.YOU on 2016/9/21.
  */
class TimeUtilTest extends FlatSpec with Matchers {

  it should "get right time delay in minutes" in {

    val calendar = Calendar.getInstance
    calendar.setTime(new Date)
    val now = calendar.get(Calendar.MINUTE)
    val time = TimeUtil.getTimeKey(-1)._1
    val mins = time.substring(time.length - 2).toInt
    mins - now should be (-1)

  }

}
