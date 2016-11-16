#!/usr/bin/env bash

cd ../../../../../../../

nohup spark-submit \
  --class com.kunyan.wokongsvc.realtimedata.TestSearch  \
  --master spark://61.147.114.85:7077 \
  --total-executor-cores 12 \
  --executor-cores 2 \
  --conf "spark.ui.port=10001" \
  ./target/spark_kafka-1.0-SNAPSHOT.jar ./config.xml /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/ret.txt &
  #hdfs:///user/wukun/jars/spark_kafka-1.0-SNAPSHOT.jar
 # --master spark://222.73.57.12:7077 \
  #--driver-class-path ./mysql-connector-java-5.1.18.jar \
  #--total-executor-cores 9 \
  #--executor-cores 3 \
  #--jars ./mysql-connector-java-5.1.18.jar \

#java -classpath ./target/spark_kafka-1.0-SNAPSHOT.jar com.kunyan.wokongsvc.realtimedata.TestLog
