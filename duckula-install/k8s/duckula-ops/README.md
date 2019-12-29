----
这是一个k8schart

k8s设计 

1、task/kafka-consumer/dump 做一个images, 用命令方式启单个pod
2、duckula-data.tar打一个image，并做成charts
3、在要支持的k8s群集中运行一个 job pod，作用就是 把 2的images初始化一个PVC（推荐网络格式）（支持多人读写）
4、启动任务时把2的chart启一个release,使用3初始化的PVC
待解决问题：  每个k8s集群的网络卷地址 路径   ops放集群外还是群集内（需要images）