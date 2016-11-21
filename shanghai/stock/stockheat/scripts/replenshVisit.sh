#!/usr/bin/env bash

cd ../../../../../../../

spark-submit \
  --class com.kunyan.wokongsvc.realtimedata.ReplenshVisit \
  --master spark://61.147.114.85:7077 \
  --total-executor-cores 16 \
  --driver-memory 10g \
  --conf "spark.driver.extraClassPath=/home/hadoop/spark-1.5.2-bin-hadoop2.6/jar/mysql-connector-java-5.1.18.jar" \
  --conf "spark.executor.extraClassPath=/home/hadoop/spark-1.5.2-bin-hadoop2.6/jar/mysql-connector-java-5.1.18.jar" \
  --conf "spark.ui.port=10000" \
  ./target/spark_kafka-1.0-SNAPSHOT.jar /home/wukun/work/wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/data.txt /tmp/ /home/wukun/work/wokong/config.xml 


#java -classpath ./target/spark_kafka-1.0-SNAPSHOT.jar com.kunyan.backcheck.MysqlHandle

