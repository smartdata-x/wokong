#!/usr/bin/env bash

#=============================================================================
#    Copyright (c) 2015
#    ShanghaiKunyan.  All rights reserved
#
#    Filename     : /home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/pullData.sh
#    Author       : Sunsolo
#    Email        : wukun@kunyan-inc.com
#    Date         : 2016-08-04 09:00
#=============================================================================

# 控制台输出字体颜色控制
FG_GREEN="\033[32;1m"
FG_RED="\033[31;1m"
FG_YELLOW="\033[33;1m"
END="\033[0m"

# 获取执行脚本所在目录路径
SCRIPT_DIR=`dirname $0`
# 获取执行脚本的名称
SCRIPT_NAME=`basename $0`

# 获取当天日期和当前时刻
HOUR=$(date +%H)
DAY=$(date +%d)

# 跨天时的参数设置
ZERO=0
INTER_HOUR=2

# 脚本中用到的文件、目录和IP配置
FILENAME=""
TEMPDIR="/home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata/data"
REMOTEDIR="/home/telecom/shdx/data/search"
REMOTEIP="192.168.1.253"
HDFSDIR="/user/wukun/shdx"
LOSE=$SCRIPT_DIR/data/lose.txt
LOSEBACKUP=$SCRIPT_DIR/data/lose_backup.txt
HOLD=$SCRIPT_DIR/data/hold.txt
HOLDBACKUP=$SCRIPT_DIR/data/hold_backup.txt
HOLD_IDENT=$SCRIPT_DIR/data/hold_ident
LOSE_IDENT=$SCRIPT_DIR/data/lose_ident

# 脚本中使用的hdfs命令，注意定时脚本中的可执行程序要写绝对路径
HDFSCOMMAND="/home/hadoop/hadoop-2.7.1/bin/hdfs"

#通过当前时间戳计算应当出现的文件的文件名
function computeFileName() {

  if [ $HOUR -ge $ZERO ] && [ $HOUR -le $INTER_HOUR ];then
    HOUR=$((10#$HOUR + 24))
    HOUR=$((10#$HOUR - 3))
    DAY=$((10#$DAY - 1))
    if [ $DAY -ge 1 ] && [ $DAY -le 9 ]; then
      HOUR=$(date +%Y%m)0${DAY}$HOUR
    else 
      HOUR=$(date +%Y%m)${DAY}$HOUR
    fi
  else 
    HOUR=$((10#$HOUR - 3))
    if [ $HOUR -ge $ZERO ] && [ $HOUR -le 9 ];then
      HOUR=$(date +%Y%m%d)0$HOUR
    else 
      HOUR=$(date +%Y%m%d)$HOUR
    fi
  fi

  FILENAME="shdx_"$HOUR
}

#远程从30主机拉取需要的文件，并上传到hdfs
function lazyScpFile() {

  scp wukun@$REMOTEIP:$REMOTEDIR/$1 $TEMPDIR

  if [ $? -eq 0 ]; then
    sendFileToHdfs $1
    echo $1 >> $HOLDBACKUP
  else
    echo $1 >> $LOSEBACKUP
  fi

}

function realScpFile() {

  # 标识在15分时数据文件已经被正常拉取
  if [ -f $HOLD_IDENT ]; then
    rm -f $HOLD_IDENT
    return 
  fi

  scp wukun@$REMOTEIP:$REMOTEDIR/$1 $TEMPDIR
  
  #测试拉取命令scp是否正常执行
  if [ $? -eq 0 ]; then

    sendFileToHdfs $1

    # 测试15分时数据文件是否被正常拉取
    if [ -f $LOSE_IDENT ]; then
      rm -f $LOSE_IDENT
      return
    fi

    # 标识在15分时数据文件已经被正常拉取
    touch $HOLD_IDENT

  else

    # 如果15分时数据没有被正常拉取，且45分没有被正常拉取，则写入数据丢失文件，这样防止多次写入
    if [ -f $LOSE_IDENT ]; then
      echo $1 >> $LOSEBACKUP
      rm -f $LOSE_IDENT
      return
    fi

    touch $LOSE_IDENT

  fi

}

# 上传到hdfs，并删除本地文件
function sendFileToHdfs() {

  $HDFSCOMMAND dfs -put $TEMPDIR/$1 $HDFSDIR
  rm -f $TEMPDIR/$1
}

# 计算当前小时应出现的数据文件名
computeFileName
# 拉取当前小时的文件
realScpFile $FILENAME

# 循环拉取延迟丢失的文件
cat $LOSE | while read line
do 
 lazyScpFile $line
done

# 将依然丢失的文件写入lose.txt
cat $LOSEBACKUP > $LOSE
# 将丢失后拉取到的文件写入hold.txt
cat $HOLDBACKUP >> $HOLD
# 清空丢失和找回的备份文件
: > $HOLDBACKUP
: > $LOSEBACKUP


