#           欢迎使用duckula

## duckula能做的事
<div align=center><img src="https://github.com/rjzjh/duckula/wiki/images/duckula_do.png" width = "600" height = "400" /></div>
上面的红色线都是duckula能做的活，默认也提供了相关的工具依赖包供业务使用，

这样就形成了一个从mysql->kafka->es 功能环。

## duckula架构
<div align=center><img src="https://github.com/rjzjh/duckula/wiki/images/InternalInstructions.png" width = "600" height = "400" /></div>
duckula监听binlog数据，解析后经过序列化、发送者等处理后会到达ES，完成了ES的增量部分处理。

还有一条线是从数据库全量同步到ES，完成了全量部分处理。


## 功能模块简介
duckula专注于数据的实时推送，它分为四大模块：
1. binlog在线监听
   实时监听mysql的binlog，并解析后推送kafka等存储中间件（也有es和redis等，但kafka适用最
   广泛，测试也最多）
2. binlog离线解析
   离线补数据用
3. kafka消费监听
   消费模块1推过来的数据，并把这些数据存放到ES/mysql等。(ES是重点功能，支持也最好)
4. 数据库全量导入
   把mysql的表数据全量导入到ES（暂时只支持ES，后续也会做插件化）
## 代码模块简介
- duckula-plugin-redis ............................redis发送者插件
- duckula-serializer-protobuf3 ...............pb3序列化插件
- duckula-serializer-protobuf2 ................pb2序列化插件
- duckula-serializer-thrift .........................thrift序列化插件
- duckula-common ...................................公共的引用模块
- duckula-task ...........................................binlog监听模块
- duckula-plugin-kafka .............................发送kafka发送者插件
- duckula-plugin-kafka-idempotent..........幂等方式发送到kafka
- duckula-plugin-elasticsearch .................直接发送ES的插件
- duckula-busi-filter ....................................过虑器插件（暂不用）
- duckula-dump-elasticsearch ..................全量导入ES的模块
- duckula-kafka-consumer ........................消息kafka数据的模块
- duckula-install ..........................................打包安装模块
- duckula-ops Tapestry 5 Application .......ops控制台模块
 ## 一此依赖版本(在开发时使用了下面版本，更多的版本未做测试)
- JDK8
- zookeeper   3.5.3-beta
- kafka 1.0.2
- es 6.3.2
- mysql: 5.6
- k8s   1.10.11
- tiller 2.11.0
- docker 18.09.02
##  相关资源
博客： https://blog.csdn.net/rjzjh
微信：zhoujunhui1172
邮箱：zhoujunhui@xforceplus.com
公司：上海云砺信息科技有限公司   https://github.com/xforceplus