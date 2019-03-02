## 使用的环境变量
DUCKULA_DATA： 数据卷存在的位置，默认为：/data/duckula-data
DUCKULA_HOME： duckula程序指定的位置，默认为：/opt/duckula

zk:  zk的服务地址，在docker中需要指定。非docker环境下zk.properties文件也可以设置
taskid: 要启动的任务
taskpattern  : 启动模式 process：进程或docker   k8s：采用k8s独立启动  tiller:使用tiller的charts来启动
ip： ip地址(用于分布式锁，与zk没什么关系)，在docker环境中需要指定分布式锁的ip，这样duckula才能识别docker已在哪台服务器上启动了监听
claimname: 使用的磁盘的PVC
