package kunyan.wokongsvc

import scala.collection.mutable.{HashSet, ListBuffer, HashMap}
import scala.util.Try

/**
  * Created by sijiansheng on 2016/11/21.
  */
package object realtimedata {

  type TryHashMap = Try[HashMap[String, (String, String)]]
  type TupleHashMap = (HashMap[String, ListBuffer[String]], HashMap[String, ListBuffer[String]])
  type TryTuple3HashMap = Try[(HashSet[String], (HashMap[String, String], HashMap[String, String], HashMap[String, String]))]
}
