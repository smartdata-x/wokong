/*=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/MixTool.scala
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-05-24 11:09
#    Description  : 
=============================================================================*/

 package com.kunyan.wokongsvc.realtimedata

 import scala.collection.mutable.Map

 /**
   * Created by wukun on 2016/5/23
   * 大杂烩，一些常量、拼接字符串和解析方法
   */
 object MixTool {

   type Tuple3Map = (Map[String, String], Map[String, String], Map[String, String])

   val VISIT = "stock_visit"
   val SEARCH = "stock_search"
   val FOLLOW = "stock_follow"
   val SQL = "select v_code, v_name_url, v_jian_pin, v_quan_pin from stock_info"

   def insertSql(
     table: String, 
     code: String, 
     stamp: Long, 
     count: Int):String = {

     "insert into " + table + " values(\'" + code + "\'," + stamp + "," + count + ");"
   }

   def insertSql(
     code: String, 
     stamp: Long, 
     count: Int): String = {

     "insert into stock_follow values(\'" + code + "\'," + stamp + "," + count + ");"
   }

   def insertSql(stamp: Long): String = {
     "insert into heat_update_time values(" + stamp + ");"
   }

   def fileName: String = {
     Thread.currentThread.getStackTrace()(2).getFileName
   }

   def rowNum: Int = {
     Thread.currentThread.getStackTrace()(2).getLineNumber
   }

   /**
     * 将不同类型的股票进行归类
     * @param stockString 股票字符串
     * @author wukun
     */
   def stockClassify(
     stockString: String, 
     alias: Tuple3Map): ((String, String), String) = {

     val elem = stockString.split("\t")

     if(elem.size != 3) {
       (("0", "0"), "0")
     } else {
       val tp = elem(2).toInt
       val mappedType = {
         if(tp >=0 && tp <= 5)
           ((elem(0), "1"), elem(1))
         else if(tp >= 6 && tp <= 9) {
           val stockCode = DataPattern.stockCodeMatch(elem(0), alias)
           ((stockCode, "2"), elem(1))
         }
         else 
           (("0", "0"), "0")
       }

       mappedType
     }
   }
 }
