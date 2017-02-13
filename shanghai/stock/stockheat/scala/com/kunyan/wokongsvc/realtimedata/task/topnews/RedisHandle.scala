package com.kunyan.wokongsvc.realtimedata.task.topnews

import com.kunyan.wokongsvc.realtimedata
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Created by sijiansheng on 2017/1/16.
  */
class RedisHandler private(ip: String, port: Int, auth: String, db: Int) {

  var jedisPool: JedisPool = null

  def getJedis: Jedis  =  {

    if (jedisPool == null) {
      throw new NullPointerException()
    }

    jedisPool.getResource
  }

  def getPool: JedisPool = jedisPool

}

object RedisHandler {

  private var redisHandler: RedisHandler = null

  def getInstance(): RedisHandler = {

    if (redisHandler == null)
      realtimedata.logger.error("init first")

    redisHandler
  }

  def init(ip: String, port: Int, auth: String, db: Int): Unit = {

    if (redisHandler != null) {
      realtimedata.logger.warn("Already init")
      return
    }

    redisHandler = new RedisHandler(ip, port, auth, db)

    val config: JedisPoolConfig = new JedisPoolConfig
    config.setMaxWaitMillis(10000)
    config.setMaxIdle(10)
    config.setMaxTotal(1024)
    config.setTestOnBorrow(true)

    redisHandler.jedisPool = new JedisPool(config, ip, port, 20000, auth, db)

    sys.addShutdownHook {
      redisHandler.jedisPool.close()
    }

  }

  def close(): Unit = {
    if (redisHandler != null) redisHandler.jedisPool.close()
  }

}