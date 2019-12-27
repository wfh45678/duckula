中间件相关配置：

规则
1、配置文件需要放到各自文件夹， es-*.properties 放到 ./es ，kafka-*.properties放到./kafka , redis-*.properties 放到 ./redis 目录中
2、命名规则  中间件名-识别ID  如: es-dev.properties 如有多个，识别ID不相同就可以
3、可以直接把文件放到duckula服务器的对应目录即可工作，如果是k8s，可以放到PVC对应目录，也可以在charts的value里进行配置
4、可以分环境进行配置，支持4套环境，但 duckula-install/pom.xml 配置"env配置"，打包时使用 conf-环境变量 文件夹的配置 
   dev("开发环境"),test("测试环境"),pre("预发环境"),prd("生产环境")
5、dev为ops在k8s运行中的默认配置示例，test为开发时的默认配置示例