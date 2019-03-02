
@rem ====================================================================== ::
@rem                                                                          
@rem  Duckula-task 任务启动脚本	
@rem  %1 taskId
@rem  %2 jmx port
@rem  %3  debug port
@rem                                                                          
@rem ====================================================================== ::
:: 2018-03-27 by andy.zhou

@echo off
echo "start"

if  "%1"=="" (
   echo "please input params taskId and jxmPort" 
   goto end 
)

echo "========================access folder======================================"
@rem 切换盘符
d:
cd %DUCKULA_DATA%/logs/task
if not exist %DUCKULA_DATA%/logs/task/%1 (  
   mkdir %1
) 

cd ../gc
echo  cur=%cd%
if not exist %DUCKULA_DATA%/logs/gc/%1 (  
   mkdir %1
) 

@rem jdk8不支持　-XX:PermSize=32m -XX:MaxPermSize=32m
@rem 变量写外面会把“号也做为字符串放入
set "JAVA_MEM_OPTS=-Xms256m -Xmx256m  -XX:PermSize=32m -XX:MaxPermSize=32m"

@rem 远程调试
if not "%3"=="" (
  set "JAVA_DEBUGGER=-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=%3"
)       

set "JAVA_JMX=-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=%2 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.ssl=false"
set "JAVA_DEFAULT_OPT=-server -XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:%DUCKULA_DATA%/logs/gc/%1/gc.log"
set "LOG_ROOT=-DlogRoot=%DUCKULA_DATA%/logs/task/%1"

set JAVA_OPTS=%JAVA_MEM_OPTS% %JAVA_JMX% %JAVA_DEFAULT_OPT% %LOG_ROOT% %JAVA_DEBUGGER%
echo  cmdstr="java  %JAVA_OPTS%  -jar %DUCKULA_HOME%/duckula-task.jar  %1"
echo "============================serverice started============================================="
@rem  start javaw  %JAVA_OPTS%  -jar %DUCKULA_HOME%/duckula-task.jar  %1
java  %JAVA_OPTS%  -jar %DUCKULA_HOME%/duckula-task.jar  %1
echo "========================serverice end======================================"

:end 
echo good bye

@rem exit