package searchandvisit

import log.PrismLogger
import redis.clients.jedis.Jedis
import scheduler.Scheduler
import util.RedisUtil

/**
  * Created by C.J.YOU on 2016/2/4.
  * search and visit data analysis
  */
object SearchAndVisit {

  /**
    * 将数据写入redis
    * @param jedis redis连接
    * @param array 数据
    * @param types 数据类别，用来区分数据来源
    */
  def writeDataToRedis(jedis:Jedis, array: Array[(String, Int)], types:String): Unit = {

    val jedis = RedisUtil.getRedis(Scheduler.confInfoMap("ip"), Scheduler.confInfoMap("port"), Scheduler.confInfoMap("auth"), Scheduler.confInfoMap("database"))

    val pipeSearchAndVisit = jedis.pipelined()

    array.foreach(line => {

      if (line._1.startsWith("hash:")) {

        val firstKeys: Array[String] = line._1.split(",")
        val keys: Array[String] = firstKeys(0).split(":")
        val outKey: String = keys(1) + ":" + keys(2)
        val dayKey = keys(2).split(" ")

        if(types != "all") {

          pipeSearchAndVisit.hincrBy(types+":"+outKey, firstKeys(1), line._2)
          pipeSearchAndVisit.expire(types+":"+outKey, 50 * 60 * 60)
          pipeSearchAndVisit.zincrby(types+":"+"set:" + keys(1) + ":" + dayKey(0), line._2, firstKeys(1))
          pipeSearchAndVisit.expire(types+":"+"set:" + keys(1) + ":" + dayKey(0), 50 * 60 * 60)
          pipeSearchAndVisit.incrBy(types+":"+"set:"+ keys(1)+":count:" + dayKey(0), line._2)
          pipeSearchAndVisit.expire(types+":"+"set:"+ keys(1)+":count:" + dayKey(0), 50 * 60 * 60)

        } else {

          pipeSearchAndVisit.hincrBy(outKey, firstKeys(1),line._2)
          pipeSearchAndVisit.expire(outKey, 50 * 60 * 60)
          pipeSearchAndVisit.zincrby("set:" + keys(1) + ":" + dayKey(0), line._2, firstKeys(1))
          pipeSearchAndVisit.expire("set:" + keys(1) + ":" + dayKey(0), 50 * 60 * 60)
          pipeSearchAndVisit.incrBy("set:"+ keys(1)+":count:" + dayKey(0), line._2)
          pipeSearchAndVisit.expire("set:"+ keys(1)+":count:" + dayKey(0), 50 * 60 * 60)

        }
      } else {

        if(types != "all") {

          pipeSearchAndVisit.incrBy(types+":"+line._1, line._2)
          pipeSearchAndVisit.expire(types+":"+line._1, 50 * 60 * 60)

        } else {

          pipeSearchAndVisit.incrBy(line._1, line._2)
          pipeSearchAndVisit.expire(line._1, 50 * 60 * 60)

        }
      }
    })

    val response = pipeSearchAndVisit.syncAndReturnAll()

    if(response.isEmpty) {
      PrismLogger.warn("Pipeline error: redis no response......")
    } else {

      val iterator = response.iterator()
      var count = 0

      while(iterator.hasNext) {

        count +=1
        iterator.next()

      }

      PrismLogger.warn("writeDataToRedis redis Response number:"+ count)

    }

    jedis.close()
    PrismLogger.warn("---pipeSearchAndVisit---syncAndReturnAll()----")

  }

}
