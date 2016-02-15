package follow

import java.io.{IOException, File}
import java.util

import _root_.util.{FileUtil, TimeUtil}
import config.FileConfig
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by C.J.YOU on 2016/1/21.
  * Follow
  */
object FollowStock {

  def writeToRedis(jedis:Jedis, stockCodes: util.List[String], followMap:mutable.HashMap[String, ListBuffer[String]]): Unit = {
    val mapIterator = followMap.iterator
    val followPipeline = jedis.pipelined()
    while(mapIterator.hasNext) {
      val followStockCodes = mapIterator.next()._2
      val timeStamp = mapIterator.next()._1
      for(follow <- followStockCodes){
        if (stockCodes.contains(follow)) {
          val followCount = "follow:count:" + TimeUtil.getTime(timeStamp)
          followPipeline.incrBy(followCount, 1)
          followPipeline.expire(followCount, 50 * 60 * 60)
          /** set follow */
          val setSearchCount = "set:follow:count:" + TimeUtil.getDate(timeStamp)
          val  setFollow = "set:follow:" + TimeUtil.getDate(timeStamp)
          followPipeline.incrBy(setSearchCount, 1)
          followPipeline.expire(setSearchCount, 50 * 60 * 60)
          followPipeline.zincrby(setFollow, 1, follow)
          followPipeline.expire(setFollow, 50 * 60 * 60)
          /** hash follow */
          writeFollowList(followPipeline,follow, timeStamp)
        }
      }
    }
    followPipeline.syncAndReturnAll()
    println("<<<<<<<<-- followPipeline ---->>>>>>>syncAndReturnAll ##### over ")
  }
  def writeFollowList(p:Pipeline,stockCodeFormat:String ,timeStamp:String ){
    val keys ="follow:"+stockCodeFormat+":"+TimeUtil.getTime(timeStamp)
    val hashKeys = "follow:"+TimeUtil.getTime(timeStamp)
    val timeSplit = TimeUtil.getTime(timeStamp).split(" ")
    val timekeys = timeSplit(0).split("-")
    val fileName = timekeys(0)+timekeys(1)+timekeys(2)+timeSplit(1)
    p.incrBy(keys, 1)
    p.expire(keys, 50*60*60)
    p.hincrBy(hashKeys,stockCodeFormat,1)
    p.expire(hashKeys, 50*60*60)

    val followFile:File  = new File(FileConfig.FOLLOW_BACKUP_FILE,"follow_"+fileName)
    // val followFile:File  = new File(FileConfig.TEST_FOLLOW_BACKUP_FILE,"follow_"+fileName)
    if(!followFile.exists()){
      try {
        followFile.createNewFile()
      } catch{
        case e:IOException =>
        e.printStackTrace()
      }
    }
  }

  /** 写入新增的follow数到redis 中 **/
  def writeAddFollowDataToRedis(jedis:Jedis,list:mutable.MutableList[String]): Unit ={
    val p = jedis.pipelined()
    var fileName = new String
    println("add follow list size: "+list.size)
    for(line <- list){
      val splits = line.split("\t")
      val stockCode = splits(0)
      var addValue = splits(1)
      val endHour  = splits(2)
      val dayKey = endHour.split(" ")
      if(addValue.toLong < 0){
        addValue = "0"
      }
      p.hincrBy("add_follow:"+endHour,stockCode,addValue.toLong)
      p.expire("add_follow:"+endHour, 50 * 60 * 60)
      p.incrBy("add_follow:"+stockCode+":"+endHour,addValue.toLong)
      p.expire("add_follow:"+stockCode+":"+endHour, 50 * 60 * 60)
      p.incrBy("add_follow:count:"+endHour,addValue.toLong)
      p.expire("add_follow:count:", 50 * 60 * 60)
      /** set follow -> add_follow is key **/
      p.zincrby("set:add_follow:" + dayKey(0),addValue.toDouble,stockCode)
      p.expire("set:add_follow:" + dayKey(0), 50 * 60 * 60)
      /* 添加当天的set count */
      p.incrBy("set:add_follow:count:" + dayKey(0),addValue.toLong)
      p.expire("set:add_follow:count:" + dayKey(0), 50 * 60 * 60)
      /** 建立follow 的备份空文件 */
      val timekeys = dayKey(0).split("-")
      fileName = timekeys(0)+timekeys(1)+timekeys(2)+dayKey(1)
    }
    System.out.println("---add_F---CRTEAT Follow File for back_up to mysql----")
    FileUtil.createFile(FileConfig.FOLLOW_BACKUP_FILE+"/follow_"+fileName,"content not used")
    // val followFile:File  = new File(FileConfig.FOLLOW_BACKUP_FILE,"follow_"+fileName)
    // val followFile:File  = new File(FileConfig.TEST_FOLLOW_BACKUP_FILE,"follow_"+fileName)
    val response = p.syncAndReturnAll()
    if(response.isEmpty){
      println("Pipeline error: redis no response...")
    }else{
      val iterator = response.iterator()
      var count = 0
      while(iterator.hasNext){
        count +=1
        val value = iterator.next()
      }
      println("writeAddFollowDataToRedis redis Response number:"+count)
    }
    System.out.println("---add_F---p.syncAndReturnAll()----")
  }

}
