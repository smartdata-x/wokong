package spark.lengjing3;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Serializable;

public final class FlatMap implements FlatMapFunction<String,String>,Serializable{
  /**
   * FlatMap
   */
  private static final Pattern tab = Pattern.compile("\t");
 
  private static final long serialVersionUID = 1L;
  private  static String stockCode = null;
  /** The time stamp. */
  private  static  String timeStamp = null;
  /** The visit web site. */
  private  static  String visitWebsite = null;
 
  private  static String getTime(String timeStamp){
    SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH" );
    BigInteger time2 = new BigInteger(timeStamp);
    String d = format.format(time2);
    return d;   
  }
  @Override
  public Iterable<String> call(String s) throws Exception {
    
    String[] lineSplits =tab.split(s);
    if(lineSplits.length < 3){
      return Arrays.asList() ;
    }
    stockCode = lineSplits[0];
    timeStamp = lineSplits[1];
    visitWebsite = lineSplits[2];
    String hour = getTime(timeStamp);
    /** visit */
    if(!("").equals(stockCode) && timeStamp != null && visitWebsite != null){
      if(visitWebsite.charAt(0) >= '0' && visitWebsite.charAt(0) <= '5'){
        return Arrays.asList("hash:visit:"+hour+","+stockCode);
     
      }else if(visitWebsite.charAt(0) >= '6' && visitWebsite.charAt(0) <= '9'){
        return Arrays.asList("hash:search:"+hour+","+stockCode);
      }
    }
  return Arrays.asList();
 }// call
}
