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
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import redis.clients.jedis.Jedis;

/**
 * The Class LoadData.
 */
public class LoadData extends Configured implements Tool{

	/**
	 * The Class LoadDataMapper.
	 */
	public static class LoadDataMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		private Jedis jedis = RedisUtil.getJedis();;
		/** The stock code. */
		private String stockCode = null;
		
		/** The time stamp. */
		private String timeStamp = null;
		
		/** The visit web site. */
		private String visitWebsite = null;
		
		private List<String> stockCodes;
		private List<String> nameUrls;
		private List<String> jianPins;
		private List<String> quanPins;
				
	
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
		   if(!jedis.isConnected()){
			   System.out.println("redis connect error");
			   System.exit(2);
		   }
		   stockCodes = jedis.lrange("stock:list", 0, -1);
		   System.out.println("redis connected");
		   for(String stockCode : stockCodes){
			   nameUrls = jedis.mget("stock:"+stockCode+":nameurl");
			   jianPins = jedis.mget("stock:"+stockCode+":jianpin");
			   quanPins = jedis.mget("stock:"+stockCode+":quanpin");
			   
		    }
		        
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
			String getLine = value.toString();
			String[] lineSplits = getLine.split("\t");
			
			if(lineSplits.length < 3){
				return ;
			}
			stockCode = lineSplits[0];
			timeStamp = lineSplits[1];
		    visitWebsite = lineSplits[2];
		    String hour = getTime(timeStamp);
			/** visit */
			if(visitWebsite.charAt(0) >= '0' && visitWebsite.charAt(0) <= '5'){
				if(stockCodes.contains(stockCode)){
				context.write(new Text("visit:"+stockCode+":"+hour), new LongWritable(1));
				
				context.write(new Text("hash:visit:"+hour+","+stockCode),new LongWritable(1)); 
				
				context.write(new Text("visit:count:"+hour), new LongWritable(1));
				}
			}
			/** search */
			else if(visitWebsite.charAt(0) >= '6' && visitWebsite.charAt(0) <= '9'){
				/** wait for finish */
				String keyWord = stockCode;
				if(keyWord.length() < 4){
					return ;
				}
					
				char firstChar = keyWord.charAt(0);
				
				if(firstChar >= '0' && firstChar <= '9'){
					if(keyWord.length() < 6){
						return;
					}
						
					for(String stockCode : stockCodes){
						if(stockCode.contains(keyWord)){
							context.write(new Text("search:"+stockCode+":"+hour), new LongWritable(1));
							context.write(new Text("hash:search:"+hour+","+stockCode),new LongWritable(1)); 
							context.write(new Text("search:count:"+hour), new LongWritable(1));
						}
						
					}
				}else if (firstChar == '%'){
					boolean b = Pattern.matches("%.*%.*%.*%.*%.*%.*%.*%",keyWord);
					if(!b){
						return;
					}
					keyWord = keyWord.toUpperCase();
					int index = 0;
					for(String nameUrl : nameUrls){
						index += 1;
						if(nameUrl.contains(keyWord)){
							context.write(new Text("search:"+stockCodes.get(index - 1)+":"+hour), new LongWritable(1));
							context.write(new Text("hash:search:"+hour+","+stockCodes.get(index - 1)),new LongWritable(1));
							context.write(new Text("search:count:"+hour), new LongWritable(1));
						}
						
				}
				
				}else if((firstChar >= 'A' && firstChar <= 'Z') || (firstChar >= 'a' && firstChar <= 'z')){
					keyWord = keyWord.toLowerCase();
					int index = 0;
					for(String jianPin : jianPins){
						index += 1;
						if(jianPin.contains(keyWord)){
							context.write(new Text("search:"+stockCodes.get(index - 1)+":"+hour), new LongWritable(1));
							context.write(new Text("hash:search:"+hour+","+stockCodes.get(index - 1)),new LongWritable(1));
							context.write(new Text("search:count:"+hour), new LongWritable(1));
						}
					}
					if(index != 0){
						return;
					}
					for(String quanPin : quanPins){
						index += 1;
						if(quanPin.contains(keyWord)){
							context.write(new Text("search:"+stockCodes.get(index - 1)+":"+hour), new LongWritable(1));
							context.write(new Text("hash:search:"+hour+","+stockCodes.get(index - 1)),new LongWritable(1));
							context.write(new Text("search:count:"+hour), new LongWritable(1));
						}
					}
				}
			
		 }	
  }
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (jedis != null) {
	            jedis.close();
	        }
		}
		/**
		 * Gets the time
		 */
		private String getTime(String timeStamp){
			SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH" );
			BigInteger time2 = new BigInteger(timeStamp);
			String d = format.format(time2);
			return d;
			
		}
	
 }

	public static class LoadDataReducer extends Reducer<Text, LongWritable, Text, Text>{
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> value,
				Reducer<Text, LongWritable, Text, Text>.Context context) throws IOException, InterruptedException {
			long counter = 0;
			long hashCounter = 0;
			if(key.toString().startsWith("hash:")){
				for ( LongWritable item : value){
					hashCounter += item.get();
				}
				String[] keys = key.toString().split(",");
				
				context.write(new Text(keys[0]), new Text(keys[1]+":"+hashCounter));
			}
			else{
				
				for ( LongWritable items : value){
					counter += items.get();
				}
				
				context.write(key, new Text(String.valueOf(counter)));
			}
			
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = null;
		String jobName = "start_job_"+System.currentTimeMillis();
		try {
			job = Job.getInstance(conf, jobName);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		/** set main class */
		job.setJarByClass(LoadData.class);
		/** set Mapper */
		job.setMapperClass(LoadDataMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		try {
			FileInputFormat.setInputPaths(job, new Path(args[0]));
		} catch (IllegalArgumentException e) {			
			e.printStackTrace();
		} catch (IOException e) {				
			e.printStackTrace();
		}
		
		/** set reducer */
		Path out = new Path(jobName+".out");
		FileOutputFormat.setOutputPath(job,out);
		job.setReducerClass(LoadDataReducer.class);
		
		/** 定制MR程序的结果输出到 Redis */
		job.setOutputFormatClass(RedisOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * The main method.
	 */
	public static void main(String[] args) throws Exception {		
		Configuration conf = new Configuration();
		if(args.length < 2){
			System.err.println("Usage: <in> <out>");
			System.exit(2);
		}
		int res = ToolRunner.run(conf, new LoadData(), args);
		System.exit(res);
	}

}
