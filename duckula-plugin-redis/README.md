1、lombok支持：
去下载lombok.jar： https://projectlombok.org/download.html  运行它选中eclipse,更新，启动 eclipse快捷方式加参数： -javaagent:lobok.jar

2、binlog执行时需要jar包可执行：
mvn dependency:copy-dependencies -DoutputDirectory=target/lib package  -Dmaven.test.skip=true -Dmaven.javadoc.skip=true
