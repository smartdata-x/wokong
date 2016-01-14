package spark.lengjing3;

import java.math.BigInteger;
import java.text.SimpleDateFormat;

public class TimeUtil {
  public static String getTime(String timeStamp){
    SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH" );
    BigInteger time2 = new BigInteger(timeStamp);
    String d = format.format(time2);
    return d;   
  }
  public static String getDate(String timeStamp){
    SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd" );
    BigInteger time2 = new BigInteger(timeStamp);
    String d = format.format(time2);
    return d;   
  }
  public static boolean isBetweenOneHour(String start,String end){
    BigInteger time1 = new BigInteger(start);
    BigInteger time2 = new BigInteger(end);
    long longstart =time1.longValue();
    long longend =time2.longValue();  
    // System.out.println("longend_----:"+longend);
    long between =(longend - longstart)/1000;
    // System.out.println("between----:"+between);
    long oneHourSeconds = 7200;
    if(between > oneHourSeconds){
      return false;
    }else{
      return true;
    }
  }
}
