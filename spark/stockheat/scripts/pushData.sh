#!/usr/bin/env bash

ZERO=0
FOUR=3

HALF=$(date +%H)
DAY=$(date +%d)

if [ $HALF -ge $ZERO ] && [ $HALF -le $FOUR ];then
  HALF=$((10#$HALF + 24))
  HALF=$((10#$HALF - 4))
  DAY=$((10#$DAY - 1))
  if [ $DAY -ge 1 ] && [ $DAY -le 9 ]; then
    HALF=$(date +%Y%m)0${DAY}$HALF
  else 
    HALF=$(date +%Y%m)${DAY}$HALF
  fi
else 
  HALF=$((10#$HALF - 4))
  if [ $HALF -ge $ZERO ] && [ $HALF -le 9 ];then
    HALF=$(date +%Y%m%d)0$HALF
  else 
    HALF=$(date +%Y%m%d)$HALF
  fi
fi

FILENAME="shdx_"$HALF
#/home/hadoop/hadoop-2.7.1/bin/hdfs dfs -rm  /user/wukun/shdx/*

/home/hadoop/hadoop-2.7.1/bin/hdfs dfs -put /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/data/shdx_2016061412 /user/wukun/shdx

rm -f /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/data/shdx_2016061412
