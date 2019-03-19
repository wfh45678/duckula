#!/bin/bash

### ====================================================================== ###
##                                                                          
##  docker打任务和推镜像	
##  $1 workdir
##  $2 version   
##  $3 needpush
##                                                                     
### ====================================================================== ###
### 2018-10-03 by andy.zhou

workdir=$1
version=$2
#images的分组,hub用"rjzjh"
group="192.168.137.100:5000/rjzjh"
#登陆地址，hub为空
loginurl="https://hub.docker.com/"
## hub.docker.com 上的用户名、密码
username_push="rjzjh"
pwd_push=""
echo "------docker build------------"
docker build -f ${workdir}/Dockerfile-data  -t="${group}/duckula:data.${version}" ${workdir}
docker build -f ${workdir}/Dockerfile-ops   -t="${group}/duckula:ops.${version}"  ${workdir}
docker build -f ${workdir}/Dockerfile-task  -t="${group}/duckula:task.${version}" ${workdir}

if [ $# -ge 3 ]; then
	echo "------login ------------"
	#docker login ${loginurl}  -u ${username_push} -p ${pwd_push}
	echo "------push ------------"
	docker push ${group}/duckula:data.${version}
	docker push ${group}/duckula:ops.${version}
	docker push ${group}/duckula:task.${version}
	echo "------k8s chart ------------"
fi

