#!/usr/bin/env bash

cd ../../../../../../../

#hdfs dfs -rm /user/wukun/jars/mysql-connector-java-5.1.18.jar
#hdfs dfs -put ./mysql-connector-java-5.1.18.jar /user/wukun/jars/

#hdfs dfs -rm /user/wukun/jars/spark_kafka-1.0-SNAPSHOT.jar
#hdfs dfs -put ./target/spark_kafka-1.0-SNAPSHOT.jar /user/wukun/jars/

spark-submit \
  --class com.kunyan.wokongsvc.realtimedata.SparkHbase  \
  --master spark://$1:7077 \
  --total-executor-cores 9 \
  --executor-cores 3 \
  --conf "spark.driver.extraClassPath=/home/hadoop/spark-1.5.2-bin-hadoop2.6/jar/mysql-connector-java-5.1.18.jar" \
  --conf "spark.executor.extraClassPath=/home/hadoop/spark-1.5.2-bin-hadoop2.6/jar/mysql-connector-java-5.1.18.jar" \
  ./target/spark_kafka-1.0-SNAPSHOT.jar /home/wukun/work/Wokong/log4j.properties ./config.xml
  #hdfs:///user/wukun/jars/spark_kafka-1.0-SNAPSHOT.jar &
  #file:///home/wukun/work/Wokong/target/spark_kafka-1.0-SNAPSHOT.jar
 # --master spark://222.73.57.12:7077 \
  #--driver-class-path ./mysql-connector-java-5.1.18.jar \
  #--total-executor-cores 9 \
  #--executor-cores 3 \
  #--jars ./mysql-connector-java-5.1.18.jar \

#java -classpath ./target/spark_kafka-1.0-SNAPSHOT.jar com.kunyan.wokongsvc.realtimedata.TestLog
