
@rem ====================================================================== ::
@rem                                                                          
@rem  Duckula-dump 任务启动脚本	
@rem  %1 dumpId
@rem  %2 jmx port
@rem  %3  debug port
@rem  %4 startId
@rem  %5 recordNum
@rem                                                                          
@rem ====================================================================== ::
:: 2018-03-27 by andy.zhou

@echo off
echo "start"

if  "%1"=="" (
   echo "please input params dumpId and jxmPort" 
   goto end 
)

echo "========================access folder======================================"
@rem 切换盘符
d:
cd %DUCKULA_DATA%/logs/dump
if not exist %DUCKULA_DATA%/logs/dump/%1 (  
   mkdir %1
) 

cd ../gc
echo  cur=%cd%
if not exist %DUCKULA_DATA%/logs/gc/%1 (  
   mkdir %1
) 

@rem jdk8不支持　-XX:PermSize=32m -XX:MaxPermSize=32m
@rem 变量写外面会把“号也做为字符串放入
set "JAVA_MEM_OPTS=-Xms512m -Xmx2048m  -XX:PermSize=32m -XX:MaxPermSize=32m"

@rem 远程调试
if not "%3"=="" (
  set "JAVA_DEBUGGER=-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=%3"
)

@rem startId
set "startId=null"
if not "%4"=="" (
  set "startId=%4"
)

@rem recordNum
set "recordNum=-1"
if not "%5"=="" (
  set "recordNum=%5"
)        

set "JAVA_JMX=-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=%2 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.ssl=false"
set "JAVA_DEFAULT_OPT=-server -XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:%DUCKULA_DATA%/logs/gc/%1/gc.log"
set "LOG_ROOT=-DlogRoot=%DUCKULA_DATA%/logs/dump/%1"

set JAVA_OPTS=%JAVA_MEM_OPTS% %JAVA_JMX% %JAVA_DEFAULT_OPT% %LOG_ROOT% %JAVA_DEBUGGER%
echo  cmdstr="java  %JAVA_OPTS%  -jar %DUCKULA_HOME%/duckula-dump-elasticsearch.jar  %1"
echo "============================serverice started============================================="
@rem  start javaw  %JAVA_OPTS%  -jar %DUCKULA_HOME%/duckula-dump-elasticsearch.jar  %1
java  %JAVA_OPTS%  -jar %DUCKULA_HOME%/duckula-dump-elasticsearch.jar  %1 %startId% %recordNum%
echo "========================serverice end======================================"

:end 
echo good bye

@rem exit