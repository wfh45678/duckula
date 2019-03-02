所有的yaml文件都需要跟据实际情况配置好了相关的yaml，然后在k8s中创建相关资源
data.yaml :实现初始化的存储PVC
ops.yaml: 实现ops发布
task.yaml: 实现mysql的binlog监听 和  kafka的consumer监听
dump.yaml: 实现全量导入的dump任务
