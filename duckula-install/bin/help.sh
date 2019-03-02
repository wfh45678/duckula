#!/bin/bash
echo ------------------------欢迎使用--duckula-------------------------------------------------------------------------------------------------------------------------
echo 'docker run -e "zk=localhost:2181" -p 61417:2723  -v /data/duckula-data:/data/duckula-data duckula:task.1 docker-run.sh  binlog_test_db-user_info 2723'
echo 'zk配置一定要与ops连接相同的'
echo '2723是docker内jmx端口号'
echo '/data/duckula-data 是images时存入的配置信息'
echo 'docker-run.sh  还可以填：docker-consumer.sh、docker-dump.sh'
echo 'binlog_test_db-user_info  ops中配置的taskId'
echo ---------------------------------------------------------------------------------------------------------------------------------------------------------------------