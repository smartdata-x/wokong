package util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}


/**
  * Created by Administrator on 2016/1/8.
  * 时间处理类
  */
object TimeUtil {
  /**
    * 获取当天日期：格式yyyy-MM-dd
    * @return 格式化日期
    */
  def getDay: String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date: String = sdf.format(new Date)
    date
  }

  /**
    * 获取当前小时
    * @return 小时
    */
  def getCurrentHour: Int = {
    val calendar = Calendar.getInstance
    calendar.setTime(new Date)
    calendar.get(Calendar.HOUR_OF_DAY)
  }

  /**
    * 获取当前时间戳
    * @return 时间戳
    */
  def getTimeStamp:Long = {
    System.currentTimeMillis()
  }
}
