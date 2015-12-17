/**
 * @author C.J.YOU
 * @date 2015年12月11日
 
 * Copyright (c)  by ShangHai KunYan Data Service Co. Ltd ..  All rights reserved.

 * By obtaining, using, and/or copying this software and/or its
 * associated documentation, you agree that you have read, understood,
 
 * and will comply with the following terms and conditions:

 * Permission to use, copy, modify, and distribute this software and
 * its associated documentation for any purpose and without fee is
 * hereby granted, provided that the above copyright notice appears in
 * all copies, and that both that copyright notice and this permission
 * notice appear in supporting documentation, and that the name of
 * ShangHai KunYan Data Service Co. Ltd . or the author
 * not be used in advertising or publicity
 * pertaining to distribution of the software without specific, written
 * prior permission.
 *
 */
package hadoop.lengjing.mr;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public final class RedisUtil {
	    
	    /** Redis服务器IP */
	    private static String address;
	    
	    /** Redis的端口号  */
	    private static int port;
	    
	    /** 访问密码  */
	    private static String auth;
	    
	    private static int database;
	    
	    /** 可用连接实例的最大数目，默认值为8,如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。*/
	    private static final int MAX_ACTIVE = 1024;
	    
	    /** 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。 */
	    private static final int MAX_IDLE = 200;
	    
	    /** 等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException； */
	    private static final int MAX_WAIT = 10000;
	    
	    private static final int TIMEOUT = 10000;
	    
	    /** 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的； */
	    private static boolean TEST_ON_BORROW = true;
	    
	    private static JedisPool jedisPool = null;
	    
	    static {
	    Properties prop =  new  Properties();    
        InputStream in = Object.class.getResourceAsStream("/conf.properties");  
         try  {    
            prop.load(in);    
            address = prop.getProperty("ip").trim();    
            port = Integer.parseInt(prop.getProperty("port").trim());
            auth = prop.getProperty("auth").trim(); 
            database = Integer.parseInt(prop.getProperty("database").trim()); 
         }catch  (IOException e) {    
            e.printStackTrace();    
        }    
      }   
	    
	    /**
	     * 初始化Redis连接池
	     */
	    static {
	        try {
	            JedisPoolConfig config = new JedisPoolConfig();
	            config.setMaxIdle(MAX_IDLE);
	            config.setMaxTotal(MAX_ACTIVE);
	            config.setMaxWaitMillis(MAX_WAIT);
	            config.setTestOnBorrow(TEST_ON_BORROW);
	            jedisPool = new JedisPool(config, address, port, TIMEOUT, auth, database);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	    }
	    
	    /**
	     * 获取Jedis实例
	     */
	    public synchronized static Jedis getJedis() {
	        try {
	            if (jedisPool != null) {
	                Jedis resource = jedisPool.getResource();
	                return resource;
	            } else {
	                return null;
	            }
	        } catch (Exception e) {
	            e.printStackTrace();
	            return null;
	        }
	    }
	    
}

