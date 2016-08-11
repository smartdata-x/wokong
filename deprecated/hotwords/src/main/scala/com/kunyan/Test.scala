package com.kunyan

import scala.collection.immutable.HashMap

/**
  * Created by yang on 4/21/16.
  */
object Test extends App {

  val map = new collection.mutable.HashMap[String, String]

  map.put("a", "1")

  println(map.get("a").get)

}
