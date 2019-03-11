#!/bin/bash

### ====================================================================== ###
##                                                                          
##  Duckula-kafka-consumer 任务启动脚本	
##  $1 consumerId
##  $2 jmx port
##  $3  debug port
##                                                                          
### ====================================================================== ###
### 2017-05-23 by andy.zhou

if [ $# -le 1 ]; then
	echo "please input params consumerId and jxmPort";
   exit
fi
cd $DUCKULA_HOME
echo "========================access folder======================================"
if [ ! -d "$DUCKULA_DATA/logs/consumer/$1" ]; then
    mkdir -p $DUCKULA_DATA/logs/consumer/$1
fi
if [ ! -d "$DUCKULA_DATA/logs/gc/$1" ]; then
    mkdir -p $DUCKULA_DATA/logs/gc/$1
fi
pwd
#jdk8不支持　-XX:PermSize=32m -XX:MaxPermSize=32m
JAVA_MEM_OPTS="-Xms256m -Xmx256m  -XX:PermSize=32m -XX:MaxPermSize=32m"
#远程调试
if [ $# -ge 3 ]; then
	JAVA_DEBUGGER="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=$3"
fi	
JAVA_JMX="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=$2 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.ssl=false"
JAVA_DEFAULT_OPT="-server -XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:$DUCKULA_DATA/logs/gc/$1/gc.log"
LOG_ROOT="-DlogRoot=$DUCKULA_DATA/logs/consumer/$1"
JAVA_OPTS="${JAVA_MEM_OPTS} ${JAVA_JMX} ${JAVA_DEFAULT_OPT} ${LOG_ROOT} ${JAVA_DEBUGGER}" 
echo  ${JAVA_OPTS}
echo "============================serverice started============================================="
nohup java  ${JAVA_OPTS}  -jar $DUCKULA_HOME/duckula-kafka-consumer.jar  $1 >/dev/null 2>&1 &
echo "========================serverice end======================================"