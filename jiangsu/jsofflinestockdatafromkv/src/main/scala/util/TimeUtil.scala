package util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Created by C.J.YOU on 2016/8/13.
  */
object TimeUtil {

  /**
    * 获取格式化日期
    * @return yyyy-MM-dd
    */
  def getDay: String = {

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date: String = sdf.format(new Date)

    date

  }

  /**
    * 获取小时
    * @return  小时
    */
  def getCurrentHour: Int = {

    val calendar = Calendar.getInstance
    calendar.setTime(new Date)
    calendar.get(Calendar.HOUR_OF_DAY)

  }

  /**
    * 获取时间戳
    * @return 时间戳 long
    */
  def getTimeStamp:Long = {
    System.currentTimeMillis()
  }


  /**
    * http请求中key的获取，精确到分，秒后续取整处理
    * @param offset 时间偏移量：+ ：将来时间偏移量， -：过去时间的偏移量
    * @return
    */
  def getTimeKey(offset: Int): (String, String) = {

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val sdfHour: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHH")
    val calendar = Calendar.getInstance
    calendar.setTime(new Date)
    calendar.set(Calendar.MINUTE, calendar.get(Calendar.MINUTE)  + offset)
    val date = calendar.getTime
    val time: String = sdf.format(date)
    val hour = sdfHour.format(date)

    (time,hour)

  }

}