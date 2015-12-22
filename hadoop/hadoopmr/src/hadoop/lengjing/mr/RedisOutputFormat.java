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
import java.math.BigInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisOutputFormat extends FileOutputFormat<Text, Text>{
	 /**
     * 定制一个RecordWriter类，每一条reduce处理后的记录，我们便可将该记录输出到数据库中
     */
  protected static class RedisRecordWriter extends RecordWriter<Text,Text> {
    private Jedis jedis = null; 
    private Pipeline p = null;
    private BigInteger index = BigInteger.ZERO;
    
    public RedisRecordWriter(Jedis jedis){
        this.jedis = jedis;
        this.p = this.jedis.pipelined(); 
        // Response<String> multi = p.multi();
        System.out.println("------Connection to server sucessfully--------");
        System.out.println("-------Server is running:------- "+jedis.ping());
    }
       
    @Override
    public void write(Text key, Text value) throws IOException,InterruptedException {
      boolean nullKey = key == null;
      boolean nullValue = value == null;
      if (nullKey || nullValue){
      	return;
      }
      System.out.println(jedis);
      
      if(key.toString().startsWith("hash:")){
      	String[] split = key.toString().split(":");
      	String[] field = value.toString().split(":");
      	String outKey = split[1]+":"+split[2];
      	p.zincrby(outKey, Long.parseLong(field[1]),field[0]);
        p.expire(outKey, 50*60*60);
      }else{
      	
      	p.incrBy(key.toString(), Long.parseLong(value.toString()));
      	p.expire(key.toString(), 50*60*60);
      	
      }
      // index = index.add(BigInteger.ONE);
      // System.out.println(index.toString());
      // BigInteger[] re = index.divideAndRemainder(new BigInteger("5"));
      // if(re[1] == BigInteger.ZERO){
      // p.exec();
      }
    }

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			if (jedis != null){
			  System.out.println("------------p.syncAndReturnAll()");
			  
			  p.syncAndReturnAll(); 
			 
				jedis.disconnect(); 
			}
			
		}

  }

	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		
	  Jedis jedis = RedisUtil.getJedis();
		return new RedisRecordWriter(jedis);
		
	}

}
