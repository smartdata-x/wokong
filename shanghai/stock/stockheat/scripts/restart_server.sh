#!/usr/bin/env bash

SCRIPT_NAME="/home/wukun/work/Wokong/src/main/scala/com/kunyan/wokongsvc/realtimedata"

VISIT_PROGRAM=$(ps -elf | grep "SparkKafka" | grep "wokongsvc" | awk '{print $4}')
FOLLOW_PROGRAM=$(ps -elf | grep "SparkHbase" | grep "wokongsvc" | awk '{print $4}')
SEARCH_PROGRAM=$(ps -elf | grep "SparkFile"  | grep "wokongsvc" | awk '{print $4}')

echo ${VISIT_PROGRAM}
echo ${FOLLOW_PROGRAM}
echo ${SEARCH_PROGRAM}

if [ ! ${VISIT_PROGRAM} ]; then
  ${SCRIPT_NAME}/sparkKafka.sh
fi

if [ ! ${FOLLOW_PROGRAM} ]; then
  ${SCRIPT_NAME}/sparkHbase.sh
fi

if [ ! ${SEARCH_PROGRAM} ]; then
  ${SCRIPT_NAME}/sparkFile.sh
fi
