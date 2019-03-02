源码安装：
1、设置好./duckula-install/pom.xml 安装选择环境<env>dev<\env>，及其它配置.
2、配置好文件 ./duckula-install/bin/docker-build.sh
3、设置好zk配置： ./duckula-install/conf-{env}/zk.properties
4、检查并修改相应的配置文件：./duckula-install/conf-{env}/*
   检查要添加的服务器jdk配置的路径，并配置： ./bin/duckula-init.sh 的变量 readonly jdkhome="/usr/lib/jvm/jdk1.8"
   配置ops, ./duckula-install/conf-{env}/duckula-ops.properties  其中较重要的参数 有:
     a、 服务器启动模式：duckula.ops.starttask.pattern
     b、 （tiller/process）docker镜像：duckula.task.image.tag  
     c、 （tiller）tiller地址：common.kubernetes.tiller.serverip
     d、 （process）服务器用户名密码： common.os.ssh.username
     e、 （离线下载）阿里云token: common.others.aliyun.server.accesskey

5、mvn clean install -Dmaven.test.skip=true -Dmaven.javadoc.skip=true
6、安装ops，下载tomcat:把  target/duckula-ops.war 复制到tomcat的 webapp目录下。把target/duckula-data.tar 解压到 /data目录下，
   设置好环境变量DUCKULA_DATA：/data/duckula-data，启动 tomcat
7、访问ops进行服务器添加：
   检查要添加的服务器jdk配置的路径，并配置： ./bin/duckula-init.sh 的变量 jdkhome="/usr/lib/jvm/jdk1.8"
8、服务器手动下载镜像：  ****/duckula:task.1.1.11  （否则程序下载会报超时错误）
   需要删除服务器上旧目录（如果有 /data/duckula-data）





docker安装ops:

   
helm下的安装ops：
1、安装PVC,指定名字： eg:"duckula"
2、在duckula目录下执行：  helm install --name=duckula --set ***=duckula  ./duckula-install/k8s/duckula_ops