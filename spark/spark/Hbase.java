package spark.lengjing3;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import scala.Tuple2;
/**
 * 
 * @author Administrator
 * @date 2015年12月30日
 
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
public final class Hbase {
  private  Map<String,Long> content = null;
  private  String tableName= null;
  private  List<Tuple2<ImmutableBytesWritable,Result>> result = null;
  private  Jedis jedis = null;
  private  Pipeline p = null;
  public Pipeline getP() {
    return p;
  }
  public void setP(Pipeline p) {
    this.p = p;
  }
  public Jedis getJedis() {
    return jedis;
  }
  public void setJedis(Jedis jedis) {
    this.jedis = jedis;
  }
  public String getTableName() {
    return tableName;
  }
  public void setTableName(String tableName) {
   this.tableName = tableName;
  }
  public List<Tuple2<ImmutableBytesWritable,Result>> getResult() {
    return result;
  }
  public void setResult(List<Tuple2<ImmutableBytesWritable,Result>> result) {
    this.result = result;
  }
  //获取配置
  public Configuration getConfiguration() {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.rootdir", "hdfs://server:9000/hbase");
    //使用eclipse时必须添加这个，否则无法定位
    conf.set("hbase.zookeeper.quorum", "server");
    conf.set(TableInputFormat.INPUT_TABLE, tableName);
    Scan scan = new Scan();
    long currentTime = System.currentTimeMillis();
    try {
	  /** 判断时间差  */
      scan.setTimeRange(currentTime-(1800000), currentTime);
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    // scan.addColumn(Bytes.toBytes("content"), null);
    ClientProtos.Scan proto = null;
    try {
      proto = ProtobufUtil.toScan(scan);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } 
    String ScanToString = Base64.encodeBytes(proto.toByteArray());
    conf.set(TableInputFormat.SCAN, ScanToString);
    return conf;
  }
  
  //读取一条记录的value
  public Map<String,Long> getValue(String tableName,String column) throws IOException{
    long timeStamp = 0;
    content = new HashMap<String,Long>();
    for (Tuple2<ImmutableBytesWritable,Result> resultTuple : result){
      byte [] value = resultTuple._2().getValue(Bytes.toBytes("basic"),Bytes.toBytes(column));
      String name = Bytes.toString(value);
      List<KeyValue> valueTimeStamp = resultTuple._2().getColumn(Bytes.toBytes("basic"),Bytes.toBytes(column));
      for(KeyValue kv : valueTimeStamp ){
         timeStamp = kv.getTimestamp();
      }
      content.put(name, timeStamp);
    }
    return content;
  }
  // get follow list
  public List<String> getFollowList(String content){
    if(content != null){
      Pattern pattern = Pattern.compile("\"\\d{6}\"");
      Matcher m = pattern.matcher(content); 
      List<String> followStockList = new ArrayList<String>();
      if(m != null){
        while (m.find()) {  
          int n = m.groupCount();  
          for (int i = 0; i <= n; i++) {  
            String outputValue = m.group(i);  
            if (outputValue != null) {  
              followStockList.add(outputValue);
            }  
          }  
        }
        return followStockList;
      }
    }
    return null;
  }
  /** get user id  */
  public String getUserId(String content){
    if(content != null){
      // System.out.println(content);
      Pattern pattern = Pattern.compile("\\[\'userid\'\\]\\s*=\\s*\'\\d{1,}\'");
      Matcher m = pattern.matcher(content); 
      if(m != null){
        if(m.find()) {  
          String outputValue = m.group(0);  
          if (outputValue != null) {  
            Pattern patternId = Pattern.compile("\\d{1,}");
            Matcher mId = patternId.matcher(outputValue); 
            if(mId.find()) {  
              String outputValueId = mId.group(0); 
              // System.out.println(outputValue); 
              return outputValueId;
            }  
          }
        }
      }
    }
    return null;
  }
  public void writeFollowList(String stockCodeFormat,String timeStamp){
    
    String keys ="follow:"+stockCodeFormat+":"+TimeUtil.getTime(timeStamp);
    // System.out.println("follow:=========:"+keys);
    String hashKeys = "follow:"+TimeUtil.getTime(timeStamp);
   
    String[] timeSplit = TimeUtil.getTime(timeStamp).split(" ");
    String[] timekeys = timeSplit[0].split("-");
    String fileName = timekeys[0]+timekeys[1]+timekeys[2]+timeSplit[1];
    File followFile = new File("/home/hadoop/unbacked_redis_files","follow_"+fileName);
    if(!followFile.exists()){
      try {
        followFile.createNewFile();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    // System.out.println("hash:follow:+++++++++:"+hashKeys);
    p.incrBy(keys, 1);
    p.expire(keys, 50*60*60);
    p.hincrBy(hashKeys,stockCodeFormat,1);
    p.expire(hashKeys, 50*60*60); 
    
   
  }
  /** 释放redis连接 **/
  public void releaseConnection(){
    if(this.jedis != null){
      if(this.p !=null){
        try {
          p.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      jedis.disconnect(); 
    }
  }
  
}
