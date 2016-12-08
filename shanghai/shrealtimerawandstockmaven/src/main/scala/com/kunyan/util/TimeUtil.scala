package com.kunyan.util

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by C.J.YOU on 2016/12/8.
  */
object TimeUtil {

  private val sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")

  def formatTimeStamp(ts: String) = sdf.format(new Date(ts.toLong))

}
