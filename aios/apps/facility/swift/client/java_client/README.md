## 编译步骤：

- 1. 安装[protobuf 2.5](../../wikis/protobuf_250_install)，用于把proto文件转成java
- 2. 修改pom.xml文件中的protobuf路径，与depend_proto的绝对路径
- 3. 运行sudo mvn compile编译
- 4. 运行sudo mvn package打包  sudo mvn package -Dmaven.test.skip=true 跳过ut  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:./
- 5. 上传java包


## 跑jar:
- java -Xbootclasspath/a:./protobuf-java-2.5.0.jar:./jna-4.2.1.jar:./swift_client-1.1.0.jar  com.alibaba.search.swift.perf.SwiftClientWriteReadPerf
- 可能存在zk地址错误与alog文件不存在问题
- 多个人同时跑topic 会相同

高级用法： client_config message_size reader_config writer_config

    java -Xbootclasspath/a:./protobuf-java-2.5.0.jar:./jna-4.2.1.jar:./swift_client-1.1.0.jar  com.alibaba.search.swift.perf.SwiftClientWriteReadPerf "zkPath=zfs://10.101.170.89:12181,10.101.170.90:12181,10.101.170.91:12181,10.101.170.92:12181,10.101.170.93:12181/swift/swift_service;logConfigFile=./swift_alog.conf;useFollowerAdmin=false" 10 topicName=sdd_test1 "topicName=sdd_test1;functionChain=HASH,hashId2partId##topicName=sdd_test2;;functionChain=HASH,hashId2partId"


 -Djna.debug_load=true  #显示加载的so
 -Djna.tmpdir=/ssd/sdd/git/swift/swift_java_client/target/  #设置临时目录