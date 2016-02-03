package util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Created by Administrator on 2016/1/7.
  */
object RedisUtil {
  val config: JedisPoolConfig = new JedisPoolConfig
  config.setMaxWaitMillis(10000)
  config.setMaxIdle(10)
  config.setMaxTotal(1024)
  config.setTestOnBorrow(true)
//   val pool = new JedisPool(config, "localhost", 6379)
//   val jedis = pool.getResource()
//   jedis.set("foo", "bar")
//  var foobar = jedis.get("foo")
//   pool.returnResourceObject(jedis)
 //  pool.destroy()
  def getRedis(ip:String,port:String,auth:String,dataBase:String):Jedis ={
    val jedisPool = new JedisPool(config,ip,port.toInt,20000,auth,dataBase.toInt)
    val jedis = jedisPool.getResource()
    jedisPool.close()
    jedis
  }
}
