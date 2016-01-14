package spark.lengjing3;

import org.apache.spark.api.java.function.PairFunction;

import scala.Serializable;
import scala.Tuple2;

public class MapPairFunction implements PairFunction<String, String, Integer>,Serializable{

  private static final long serialVersionUID = 1L;
 
  @Override
  public Tuple2<String, Integer> call(String s ) throws Exception {
    
    return new Tuple2<String, Integer>(s,1);
  }

}
