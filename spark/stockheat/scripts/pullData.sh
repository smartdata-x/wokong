#!/usr/bin/env bash

ZERO=0
FOUR=2

HALF=$(date +%H)
DAY=$(date +%d)

FILENAME=""

#通过当前时间戳计算应当出现的文件的文件名
function computeFileName() {

  if [ $HALF -ge $ZERO ] && [ $HALF -le $FOUR ];then
    HALF=$((10#$HALF + 24))
    HALF=$((10#$HALF - 3))
    DAY=$((10#$DAY - 1))
    if [ $DAY -ge 1 ] && [ $DAY -le 9 ]; then
      HALF=$(date +%Y%m)0${DAY}$HALF
    else 
      HALF=$(date +%Y%m)${DAY}$HALF
    fi
  else 
    HALF=$((10#$HALF - 3))
    if [ $HALF -ge $ZERO ] && [ $HALF -le 9 ];then
      HALF=$(date +%Y%m%d)0$HALF
    else 
      HALF=$(date +%Y%m%d)$HALF
    fi
  fi

  FILENAME="shdx_201607181"
  #FILENAME="shdx_"$HALF
}

#远程从30主机拉取需要的文件，并上传到hdfs
function scpFile() {

  scp wukun@192.168.1.30:/home/telecom/shdx/data/search/$1 /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/data

  if [ $? -eq 0 ];then
    sendFileToHdfs $1
    echo $1 >> hold_backup.txt
  else
    echo $1 >> lose_backup.txt
  fi
}

#上传到hdfs
function sendFileToHdfs() {

  /home/hadoop/hadoop-2.7.1/bin/hdfs dfs -put /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/data/$1 /user/wukun/shdx
  rm -f /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/data/$1
}

computeFileName
scpFile $FILENAME

cat ./lose.txt | while read line
do 
  scpFile $line
done

cat lose_backup.txt > lose.txt
cat hold_backup.txt > hold.txt

echo $FILENAME

: > hold_backup.txt
: > lose_backup.txt

