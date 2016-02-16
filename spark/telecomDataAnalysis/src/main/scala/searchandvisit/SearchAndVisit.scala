package searchandvisit

import redis.clients.jedis.Jedis
import scheduler.Scheduler
import util.RedisUtil

/**
  * Created by C.J.YOU on 2016/2/4.
  * search and visit data analysis
  */
object SearchAndVisit {

  def writeDataToRedis(jedis:Jedis,array: Array[(String,Int)]): Unit ={
    val jedisSv = RedisUtil.getRedis(Scheduler.confInfoMap("ip"),Scheduler.confInfoMap("port"),Scheduler.confInfoMap("auth"),Scheduler.confInfoMap("database"))
    // println("redisinfo:"+Scheduler.confInfoMap("ip")+":"+Scheduler.confInfoMap("port")+":"+Scheduler.confInfoMap("auth")+":"+Scheduler.confInfoMap("database"))
    val pipeSearchAndVisit = jedisSv.pipelined()
    array.foreach(line => {
      if (line._1.startsWith("hash:")) {
        val firstKeys: Array[String] = line._1.split(",")
        val keys: Array[String] = firstKeys(0).split(":")
        val outKey: String = keys(1) + ":" + keys(2)
        val dayKey = keys(2).split(" ")
        pipeSearchAndVisit.hincrBy(outKey, firstKeys(1),line._2)
        pipeSearchAndVisit.expire(outKey, 50 * 60 * 60)
        /* 添加当天的set list */
        pipeSearchAndVisit.zincrby("set:" + keys(1) + ":" + dayKey(0), line._2, firstKeys(1))
        pipeSearchAndVisit.expire("set:" + keys(1) + ":" + dayKey(0), 50 * 60 * 60)
        /* 添加当天的set count */
        pipeSearchAndVisit.incrBy("set:"+ keys(1)+":count:" + dayKey(0), line._2)
        pipeSearchAndVisit.expire("set:"+ keys(1)+":count:" + dayKey(0), 50 * 60 * 60)
      } else {
        pipeSearchAndVisit.incrBy(line._1, line._2)
        pipeSearchAndVisit.expire(line._1, 50 * 60 * 60)
      }
    })
    val response = pipeSearchAndVisit.syncAndReturnAll()
    if(response.isEmpty){
      println("Pipeline error: redis no response......")
    }else{
      val iterator = response.iterator()
      var count = 0
      while(iterator.hasNext){
        count +=1
        iterator.next()
      }
      println("writeDataToRedis redis Response number:"+count)
    }
    jedisSv.close()
    System.out.println("---pipeSearchAndVisit---syncAndReturnAll()----")
  }
}
