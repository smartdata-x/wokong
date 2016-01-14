
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark.lengjing3;
import scala.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Pattern;

public final class LoadData {
  
  private  static Jedis jedis = RedisUtil.getJedis();
  private  static Pipeline p;
  private  static List<String> stockCodes;
  private  static List<String> nameUrls = new ArrayList<String>();
  private  static List<String> jianPins = new ArrayList<String>();
  private  static List<String> quanPins = new ArrayList<String>();
  private  static Map<String,Long> content = new HashMap<String,Long>();
  private  static final String tableName="1";
  private static  String setSearchCount ="set:search:count:";
  private static  String setVisitCount ="set:visit:count:";
  // private static  String setFollowCount ="set:follow:count:";
  private static  String setSearch ="set:search:";
  private static  String setVisit ="set:visit:";
  // private static  String setFollow ="set:follow:";
  /** stock first char is a number */
  public static boolean isSearchOfNumber(String stockCode){
    if(("").equals(stockCode))
      return false;
    char firstChar = stockCode.charAt(0);
    if(firstChar >= '0' && firstChar <= '9'){
      if(stockCode.length() < 6){
        return false;
      }else{
        return true;
      }
    }else{
      return false;
    }
  }
  /** stock is started with '%' */
  public static boolean isStartWith(String stockCode){
    if(("").equals(stockCode))
      return false;
    char firstChar = stockCode.charAt(0);
    if (firstChar == '%'){
      Pattern pattern = Pattern.compile("%.*%.*%.*%.*%.*%.*%.*%");
      boolean b = pattern.matcher(stockCode).find();
      if(!b){
        return false;
      }else{
        return true;
      }
    }else{
      return false;
    }  
    
  }
  /** stock first char is alpha */
  public static boolean isAlpha(String stockCode){
    if(("").equals(stockCode))
      return false;
    char firstChar = stockCode.charAt(0);
    if((firstChar >= 'A' && firstChar <= 'Z') || (firstChar >= 'a' && firstChar <= 'z')){
      return true;
    }else{
      return false;
    }    
  }
  
  /** main function */
  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: LoadData <file>");
      System.exit(1);
    }
    stockCodes = jedis.lrange("stock:list", 0, -1);
    System.out.println("redis connected");
    
    for(String stockCode : stockCodes){
      String nameurl = jedis.get("stock:"+stockCode+":nameurl");
      String jianPin = jedis.get("stock:"+stockCode+":jianpin");
      String quanPin = jedis.get("stock:"+stockCode+":quanpin");
      nameUrls.add(nameurl);
      jianPins.add(jianPin);
      quanPins.add(quanPin);
    }
    p = jedis.pipelined();
    // 开始事务
    p.multi();
    
    SparkConf sparkConf = new SparkConf().setAppName("LoadData")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer.max", "2000");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = ctx.textFile(args[0]);
    // System.out.println("Usage: <in> <out> "+args[0]);
    /** flatMap */
    JavaRDD<String> words = lines.flatMap(new FlatMap());
    /** mapToPair */  
    JavaPairRDD<String, Integer> ones = words.mapToPair(new MapPairFunction());
    /** reduceByKey */  
    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new ReduceFunction());
    /** collect */
    List<Tuple2<String, Integer>> output = counts.collect();
     
    for (Tuple2<?,?> tuple : output) {
 
      if(tuple._1().toString().startsWith("hash:visit:")){
        try {
          String[] split = tuple._1().toString().split(",");
          if(split.length < 2)
            continue ;
          /** key  hash:visit:hour */
          String[] splitKeys = split[0].toString().split(":");
          /** stockCode */
          String splitfield = split[1].toString();
          if(stockCodes.contains(splitfield)){
            String value = tuple._2().toString();
            String outKey = splitKeys[1]+":"+splitKeys[2];
            String[] dayKey = splitKeys[2].split(" ");
            if(value !="0"){
              p.hincrBy(outKey,splitfield,Long.parseLong(value));
              p.expire(outKey, 50*60*60);
              
              /**visit:count  */
              p.incrBy("visit:count:"+splitKeys[2], Long.parseLong(value));
              p.expire("visit:count:"+splitKeys[2], 50*60*60);
              /** visit:stockCode:hours */
              p.incrBy("visit:"+splitfield+":"+splitKeys[2], Long.parseLong(value));
              p.expire("visit:"+splitfield+":"+splitKeys[2], 50*60*60);
              
              /** 添加当天的set list*/
              p.zincrby(setVisit+dayKey[0], Double.parseDouble(value), splitfield);
              p.expire(setVisit+dayKey[0], 50*60*60);
              /** 添加当天的set count*/
              p.incrBy(setVisitCount+dayKey[0],Long.parseLong(value));
              p.expire(setVisitCount+dayKey[0], 50*60*60);
            }
          }
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          // DISCARD：取消事务，
          p.discard();
          p.clear();
          System.out.println("pipeline is cleared");
        }
        
      }else if(tuple._1().toString().startsWith("hash:search:")){
        try {
          String[] split = tuple._1().toString().split(",");
          if(split.length < 2)
            continue ;
          /** key  hash:search:hour */
          String[] splitKeys = split[0].toString().split(":");
          /** stockCode */
          String splitfield = split[1].toString();
          if(isSearchOfNumber(splitfield)){
            for(String stockCode  :  stockCodes){
              if(stockCode.contains(splitfield)){
                String value = tuple._2().toString();         
                String outKey = splitKeys[1]+":"+splitKeys[2];
                String[] dayKey = splitKeys[2].split(" ");
                if(value !="0"){
                  p.hincrBy(outKey, stockCode, Long.parseLong(value));
                  p.expire(outKey, 50*60*60);
                 
                  /** search:count  */
                  p.incrBy("search:count:"+splitKeys[2], Long.parseLong(value));
                  p.expire("search:count:"+splitKeys[2], 50*60*60);
                  /** search:stockCode:hours */
                  p.incrBy("search:"+stockCode+":"+splitKeys[2], Long.parseLong(value));
                  p.expire("search:"+stockCode+":"+splitKeys[2], 50*60*60);
                  
                  /** 添加当天的set */
                  p.zincrby(setSearch+dayKey[0], Double.parseDouble(value), splitfield);
                  p.expire(setSearch+dayKey[0], 50*60*60);
                  /** 添加当天的set count */
                  p.incrBy(setSearchCount+dayKey[0],Long.parseLong(value));
                  p.expire(setSearchCount+dayKey[0], 50*60*60);
                }
              }
              
            }
          }else if(isStartWith(splitfield)){       
            splitfield = splitfield.toUpperCase();
            int index = 0;
            for(String nameUrl  :  nameUrls){         
              index += 1;
              if(nameUrl.contains(splitfield)){
                String value = tuple._2().toString();            
                String outKey = splitKeys[1]+":"+splitKeys[2];
                String[] dayKey = splitKeys[2].split(" ");
                if(value !="0"){
                  p.hincrBy(outKey,stockCodes.get(index-1),Long.parseLong(value));
                  p.expire(outKey, 50*60*60);
                  
                  /** search:count  */
                  p.incrBy("search:count:"+splitKeys[2], Long.parseLong(value));
                  p.expire("search:count:"+splitKeys[2], 50*60*60);
                  /** search:stockCode:hours */
                  p.incrBy("search:"+stockCodes.get(index-1)+":"+splitKeys[2], Long.parseLong(value));
                  p.expire("search:"+stockCodes.get(index-1)+":"+splitKeys[2], 50*60*60);
                  
                  /** 添加当天的set */
                  p.zincrby(setSearch+dayKey[0], Double.parseDouble(value), stockCodes.get(index-1));
                  p.expire(setSearch+dayKey[0], 50*60*60);
                  /** 添加当天的set count */
                  p.incrBy(setSearchCount+dayKey[0],Long.parseLong(value));
                  p.expire(setSearchCount+dayKey[0], 50*60*60);
                }
              }
            }
          }else if(isAlpha(splitfield)){
            splitfield = splitfield.toLowerCase();
            int index = 0;
            for(String jianPin : jianPins){
              index += 1;
              if(jianPin.contains(splitfield)){
              
                String value = tuple._2().toString();            
                String outKey = splitKeys[1]+":"+splitKeys[2];   
                String[] dayKey = splitKeys[2].split(" ");
                if(value !="0"){
                  p.hincrBy(outKey,stockCodes.get(index-1),Long.parseLong(value));
                  p.expire(outKey, 50*60*60);
                  
                  /** search:count  */
                  p.incrBy("search:count:"+splitKeys[2], Long.parseLong(value));
                  p.expire("search:count:"+splitKeys[2], 50*60*60);
                  /** search:stockCode:hours */
                  p.incrBy("search:"+stockCodes.get(index-1)+":"+splitKeys[2], Long.parseLong(value));
                  p.expire("search:"+stockCodes.get(index-1)+":"+splitKeys[2], 50*60*60);
                  
                  /** 添加当天的set */
                  p.zincrby(setSearch+dayKey[0], Double.parseDouble(value), stockCodes.get(index-1));
                  p.expire(setSearch+dayKey[0], 50*60*60);
                  /** 添加当天的set count */
                  p.incrBy(setSearchCount+dayKey[0],Long.parseLong(value));
                  p.expire(setSearchCount+dayKey[0], 50*60*60);
                }
              }
            }
            if(index != 0){
              continue ;
            }
            for(String quanPin : quanPins){
              index += 1;
              if(quanPin.contains(splitfield)){          
                String value = tuple._2().toString();              
                String outKey = splitKeys[1]+":"+splitKeys[2];    
                String[] dayKey = splitKeys[2].split(" ");
                if(value !="0"){
                  p.hincrBy(outKey,stockCodes.get(index-1),Long.parseLong(value));
                  p.expire(outKey, 50*60*60);
                  
                  /** search:count  */
                  p.incrBy("search:count:"+splitKeys[2], Long.parseLong(value));
                  p.expire("search:count:"+splitKeys[2], 50*60*60);
                  /** search:stockCode:hours */
                  p.incrBy("search:"+stockCodes.get(index-1)+":"+splitKeys[2], Long.parseLong(value));
                  p.expire("search:"+stockCodes.get(index-1)+":"+splitKeys[2], 50*60*60);
                  
                  /** 添加当天的set */
                  p.zincrby(setSearch+dayKey[0], Double.parseDouble(value), stockCodes.get(index-1));
                  p.expire(setSearch+dayKey[0], 50*60*60);
                  /** 添加当天的set count */
                  p.incrBy(setSearchCount+dayKey[0],Long.parseLong(value));
                  p.expire(setSearchCount+dayKey[0], 50*60*60);
                  
                }
              }
            }
          }
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          p.discard();
          p.clear();
          System.out.println("pipeline is cleared");
        }
      }//
      
    }
    /**stock analysis modle for redis */
    try {
      Hbase hbase = new Hbase(); 
      hbase.setTableName(tableName);
      Configuration conf = hbase.getConfiguration();
        
      JavaPairRDD<ImmutableBytesWritable,Result> myRDD =ctx.newAPIHadoopRDD(conf, TableInputFormat.class,ImmutableBytesWritable.class, Result.class);
      List<Tuple2<ImmutableBytesWritable,Result>> result = myRDD.collect();
      hbase.setResult(result);
      String column = "content";
      content = hbase.getValue(tableName, column);
      hbase.setJedis(jedis);
      hbase.setP(p);
      if(content !=null){
        Set<Entry<String,Long>> entrySet = content.entrySet();
        for(Entry<String,Long> tempContent : entrySet){
          if(hbase.getFollowList(tempContent.getKey()) != null){
            List<String> followStockList = hbase.getFollowList(tempContent.getKey());
            /** get user id */
           // String userId = hbase.getUserId(tempContent.getKey());
           // System.out.println("content:Key--"+tempContent.getKey()+"=====value:"+TimeUtil.getTime(tempContent.getValue().toString()));
           // System.out.println("userid:+++"+userId);
            for(String stockCodeFollow : followStockList){
              /** 需要去掉匹配好股票的 双引号 **/
              String stockCodeFormat = stockCodeFollow.substring(1, 7);
              String timeStamp = tempContent.getValue().toString();
              if(stockCodes.contains(stockCodeFormat)){
                
                long currentTime = System.currentTimeMillis();
                
                String followCount = "follow:count:"+TimeUtil.getTime(timeStamp);
                p.incrBy(followCount, 1);
                p.expire(followCount, 50*60*60);
                
                /**set follow */
                String setSearchCount ="set:follow:count:"+TimeUtil.getDate(timeStamp);
                String setFollow ="set:follow:"+TimeUtil.getDate(timeStamp);
                p.incrBy(setSearchCount, 1);
                p.expire(setSearchCount, 50*60*60);
                p.zincrby(setFollow,1,stockCodeFormat);
                p.expire(setFollow, 50*60*60); 
                /** hash follow */
                hbase.writeFollowList(stockCodeFormat,timeStamp);
                /**usse id follow stockCode info  */
                // if(userId != null){
                  // String userIdFollowCount = "user:"+userId+":"+TimeUtil.getDate(timeStamp);
                  // p.incrBy(userIdFollowCount, 1);
                  // p.expire(userIdFollowCount, 50*60*60);
                //}           
              }
            }
          }
        }   
      }
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      p.discard();
      p.clear();
      System.out.println("pipeline is cleared");
    }
    /** 关闭连接  **/
    if (jedis != null){
      System.out.println("------p.syncAndReturnAll()---------");
      /** 提交事务  */
      p.exec();
      p.syncAndReturnAll(); 
      jedis.disconnect(); 
    }
    ctx.close();
  }
}