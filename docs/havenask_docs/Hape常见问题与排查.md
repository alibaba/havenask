# Hape常见问题与排查

## hape工作原理
* hape做的所有操作都是基于havenask/swift/bs admin的
* admin基于hape给出可调度的机器列表和集群目标对于worker进行调整。hape不直接控制worker

## Hape运维命令报错排查方法
### 1. Hapee可以用于debug的命令
* 可以使用hape validate命令进行简单校验
* 在任意Hape命令后面加上-v可以看到具体Hape的具体行为，可以根据具体哪个行为报错来重新问题。
* 例子：发现加上-v后显示的Hape行为中涉及创建容器失败，可以复制日志中显示的创建容器命令，自己手动执行来定位问题
  
### 2. 启动进程错误排查
* 如果是Hape start命令拉起进程有问题，可能是admin容器创建异常或者admin进程启动异常。
* 如果admin创建容器异常的报错，可以用-v选项来排查具体是哪条命令行异常
* 如果是admin不ready的报错，说明是admin进程启动异常，可以使用hape gs子命令查看admin具体在哪一台机器上，查看日志最近的ERROR信息，查看日志方法见下一节引擎报错排查方法


### 3. 表运维命令错误排查
* 如果Hape的表运维命令报错，可能是表相关配置有问题。可以使用hape gs havenask子命令查看qrs和searcher在哪一台机器上，查看日志最近的ERROR信息，查看日志方法见下一节引擎报错排查方法


## 引擎报错排查方法
* 引擎admin、worker的报错都需要查看对应的引擎日志，查看方法如下
    1. 使用hape gs命令查看对应进程所在机器
    2. havenask集群默认在/home/\<user\>下创建所有进程的工作目录。可以在某一台机器下使用 ls ~ | grep havenask | grep \<serviceName\>（serviceName代表集群配置里面global.conf设定的服务名）。其中appmaster也即admin
    3. 日志：
          * 所有进程有基本日志：
              * 启动日志：\<workdir\>/stdout.log 与\<workdir\>/stdin.log
          * havenask admin的主要日志：
              * 集群进程调度日志 \<workdir\>/hippo.log
              * 集群目标日志 \<workdir\>/suez.log
          * havenask worker的主要日志: 
              * sql引擎工作日志 \<workdir\>/ha_sql/logs/sql.log
          * swift admin的主要日志：
              * 集群进程调度日志 \<workdir\>/logs/hippo.log
              * 集群目标日志 \<workdir\>/logs/swift/swift.log
          * swift broker的主要日志：
              * broker工作日志 \<workdir\>/logs/swift/swift.log
* 登录容器方法：

```
find <workdir> -name sshme
```

```
bash <path-to>/sshme
```
  
* 手动进程重启方法：
  * 先进入容器，ps -aux找到并杀死进程
  * 然后执行进程启动脚本
```
find <workdir> -name process_starter.sh
```

```
bash <path-to>/process_starter.sh & 
```



## 常见问题

### 创建进程不成功
* 容器创建不成功，Hape命令加上-v选项后发现一些权限问题
  * 避免使用root账号，确定当前账号在/home/\<user\>下有充分的权限
* 容器创建不成功，docker ps --all 显示容器退出
    * 在对应机器上执行sudo chmod 666 /var/run/docker.sock

* 创建swift topic超时
    * 一般是因为多机模式下的hadoopHome和javaHome设置不对造成的
    * 使用hape gs swift命令查看broker是否启动成功
    * 查看admin和broker日志是否有异常

* qrs未达目标行数/hape gs havenask命令显示部分进程异常
    * 可以检查一下对应日志是否是端口被占用
    * 可以使用hape restart container命令重启节点

### 向量索引
* 向量索引不生效，插入后查询得到空数据
    * 参考向量索引例子的模板/ha3_install/example/cases/vector/vector_conf，其中data_tables和普通的配置有所不同

### 多机模式下hdfs配置不成功
* 多机模式下需要确保所有机器都能通过你配置的hadoop路径访问对应地址
* 可以通过以下方法验证是否配置成功：
  * 进入容器，执行以下命令看能否成功。其中JAVA_HOME和HADOOP_HOME是你配置java与hadoop路径。DATA_STORE_ROOT是你配置的多机模式下havenask数据目录

```
JAVA_HOME=<JAVA_HOME> HADOOP_HOME=<HADOOP_HOME> /ha3_install/usr/local/bin/fs_util ls <DATA_STORE_ROOT>
```

* 如果报出权限问题，则需要用hdfs工具对于目录授予777权限


### 如何查看引擎里面的表信息
* 访问qrs的/sqlClientInfo接口


### 如何判断进程在哪里
* 执行havenask gs havenask命令可以查看进程分布在哪台机器上