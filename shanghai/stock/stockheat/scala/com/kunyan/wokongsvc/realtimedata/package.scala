package com.kunyan.wokongsvc

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  * Created by sijiansheng on 2016/11/21.
  */
package object realtimedata {

  type TryHashMap = Try[mutable.HashMap[String, (String, String)]]
  type TupleHashMap = (mutable.HashMap[String, ListBuffer[String]], mutable.HashMap[String, ListBuffer[String]])
  type TryTuple3HashMap = Try[(mutable.HashSet[String], (mutable.HashMap[String, String], mutable.HashMap[String, String], mutable.HashMap[String, String]))]
}
