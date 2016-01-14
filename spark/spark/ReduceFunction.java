package spark.lengjing3;

import org.apache.spark.api.java.function.Function2;

import scala.Serializable;

public class ReduceFunction  implements Function2<Integer, Integer, Integer>,Serializable{
  
  private static final long serialVersionUID = 1L;
  @Override
  public Integer call(Integer i1, Integer i2) throws Exception {
   
    return i1 + i2;
  }

}
