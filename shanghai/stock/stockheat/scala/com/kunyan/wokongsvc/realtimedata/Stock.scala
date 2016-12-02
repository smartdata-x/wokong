package com.kunyan.wokongsvc.realtimedata

import com.kunyan.wokongsvc.realtimedata.MixTool._
import com.kunyan.wokongsvc.realtimedata.logger.HeatLogger

import scala.collection.mutable
import scala.util.{Failure, Success}


/**
  * Created by sijiansheng on 2016/11/18.
  */
object Stock {

  var stockAlias: Tuple2Map = _

  var followStockAlias: mutable.HashSet[String] = _

  def initStockAlias(mysqlPool: MysqlPool): Unit = {

    this.stockAlias = (mysqlPool.getConnect match {

      case Some(connect) =>

        val sqlHandle = MysqlHandle(connect)
        //数据库中股票代码和别名对应
        val alias = sqlHandle.execQueryStockAlias(MixTool.SYN_SQL) match {
          case Success(z) => z
          case Failure(e) =>
            HeatLogger.exception(e)
            System.exit(-1)
        }
        sqlHandle.close()


        alias

      case None =>
        HeatLogger.error("[Get mysql connect failure]")
        System.exit(-1)
    }).asInstanceOf[Tuple2Map]
  }

  def initFollowStockAlias(mysqlPool: MysqlPool): Unit = {

    this.followStockAlias = (mysqlPool.getConnect match {
      case Some(connect) =>
        val sqlHandle = MysqlHandle(connect)

        val stock = sqlHandle.execQueryStock(MixTool.STOCK_SQL) match {
          case Success(z) => z
          case Failure(e) =>
            HeatLogger.exception(e)
            System.exit(-1)
        }

        sqlHandle.close()

        stock

      case None =>
        HeatLogger.error("[Get mysql connect failure]")
        System.exit(-1)
    }).asInstanceOf[mutable.HashSet[String]]
  }

}
