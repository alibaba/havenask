# Hape多机模式

* 多机模式是基于多台物理机调度拉起havenask集群
* 以下任何操作出现异常可以参考[Hape常见问题与排查.md](Hape常见问题与排查.md)进行排查


##  一. 准备环境和配置
### 准备环境

* 准备多台物理机
* 最新的镜像为registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:latest，不同版本的镜像不同，可以参考[release页面](https://github.com/alibaba/havenask/releases)获取镜像版本，需要确保每台物理机已经用docker拉取对应版本的镜像。
* 要求所有的机器上都有与hape执行时相同的用户，用户对于自己的home有充分的权限。不建议使用root用户
* 确保机器之间已经ssh免密打通，机器也能免密ssh登录自己，如果没有打通可以通过下面的步骤打通：
  * 检查每个机器用户目录下是否有.ssh/id_rsa.pub文件，如果没有可以通过ssh-keygen生成
  * 使用ssh-copy-id将id_rsa.pub拷贝到其他机器


### 修改配置
* 修改/ha3_install/hape_conf/remote，配置的具体各项含义可以参考[Hape配置详解](Hape配置详解.md)文档
#### hdfs配置
* 由于多机模式的havenask集群需要在集群内部不同机器之间共享配置、索引等数据，因此需要搭建hdfs文件系统。当前版本的havenask引擎需要java8，用户可以下载java8对应任意版本hadoop并拉起hdfs服务。接下来做以下配置：
  * 创建数据目录：
    * 在hdfs下新建两个目录hdfs://xxx/havenask和hdfs://xxx/swift分别用于存储havenask和swift数据
    * 使用hadoop fs -chmod 777命令对两个文件目录赋予权限
  * 配置变量：
    * global.conf文件的javaHome：指向对应java路径，需要能被容器访问，因此java目录需要在容器挂载的/home/\<user\>下的某个路径上。如果不在，可以使用软链接地址
    * global.conf文件的hadoopHome：指向对应hadoop路径，需要能被容器访问，因此hadoop目录需要在容器挂载的/home/\<user\>下的某个路径上。如果不在，可以使用软链接地址
    * global.conf文件的dataStoreRoot，对应上述的hdfs://xxx/havenask地址
    * cluster_templates/swift/config/swift.conf文件的data_root_path，对应上述的hdfs/xxx/swift地址
* 多机模式下的hdfs遇到问题可以进一步参考[多机模式下hdfs配置不成功](Hape常见问题与排查.md#多机模式下hdfs配置不成功)

#### 可调度机器列表

* 其中global.conf必须要做修改
    * swift相关：
        * 可用于调度的机器列表adminIpList & workerIpList
    * havenask相关：
        * 可用于调度的机器列表adminIpList & qrsIpList & searcherIpList
        * 其中searcher的机器数要不小于后面建表的分片数
        * 当前版本下qrs和searcher之间的机器不能复用。但是admin与qrs、searcher的机器可以复用
    * bs相关：
        * 可用于调度的机器列表adminIpList & workerIpList


## 二、创建havenask集群

* 创建并进入hape容器
```
./docker/havenask/create_container.sh <container_name> <image>
./<container_name>/sshme
```

* 验证hape运行环境无明显异常。由于多级模式不采用默认配置，因此后面所有路径都需要指定分布式集群的配置路径/ha3_install/hape_conf/remote
```
hape validate -c /ha3_install/hape_conf/remote
``` 

* 创建havenask集群
```
hape start havenask -c /ha3_install/hape_conf/remote
```


## 三. 表管理
* havenask表分为直写表和全量表两种，可以选择其中一种进行创建，或两种都创建
* 创建直写表
    * -t表示表名字
    * -p表示分片数。多机模式下，假设有N张表，其中最大分片为M，那么所有表的分片要求必须要么是1，要么是M
    * -s表示schema路径
```
hape create table -t in0 -p 2 -s /ha3_install/example/cases/normal/in0_schema.json -c /ha3_install/hape_conf/remote

```
* (启动速度慢，可选) 创建全量表
    * -t表示表名字
    * -p表示分片数。多机模式下，假设有N张表，其中最大分片为M，那么所有表的分片要求必须要么是1，要么是M
    * -s表示schema路径
    * -f表示全量文件路径，支持文件或目录。分布式要求必须把数据文件上传然后提供hdfs路径
```
## 先用hdfs工具把/ha3_install/example/data/test.data上传
## 
hape create table -t in1 -p 2 -s /ha3_install/example/cases/normal/in0_schema.json  -f hdfs://xxxx/test.data -c /ha3_install/hape_conf/remote
```
* 由于全量表需要拉起bs服务，启动多个bs任务，一般需要等待3~5分钟生效，因此可以用以下命令查看bs状态。状态含义见[Hape集群状态](Hape集群状态.md)
```
hape gs bs -c /ha3_install/hape_conf/remote

## 当带有full进程都结束，只剩下inc进程时，执行以下命令发现对应表状态为READY
hape gs table -t <table_name> -c /ha3_install/hape_conf/remote
```

* 如果想要删除表，可以执行以下操作
```
hape delete table -t <table-name> -c /ha3_install/hape_conf/remote
```

## 四. 查看集群状态
* 在完成建表后，可以使用以下命令查看集群状态，判断集群是否已经准备好可以读写。状态含义见[Hape集群状态](Hape集群状态.md)
```
## 当返回的结果中，sqlClusterInfo的clusterStatus为READY时集群已经准备好
hape gs havenask -c /ha3_install/hape_conf/remote
```


## 五. 读写数据

* 直写表写入数据命令如下。
```
## 由于qrs可能在远端拉起，因此需要先用hape gs havenask子命令来获取qrs的地址
/ha3_install/sql_query.py  --address  http://<qrs-ip>:45800 --query "insert into in0 (createtime,hits,id,title,subject)values(1,2,4,'测试', '测试')"
```

* 查询表数据
```
/ha3_install/sql_query.py  --address  http://<qrs-ip>:45800 --query "select * from in0" 
```

* 如果想要查看text类型字段值，则需要联合查询[摘要索引](摘要索引.md)，命令如下
```
/ha3_install/sql_query.py --address  http://<qrs-ip>:45800 --query "select in0_summary_.id, in0_summary_.title, in0_summary_.subject from in0 inner join in0_summary_ on in0.id=in0_summary_.id"
```


* 全量表写实时数据推送需要swift。目前推荐使用swift c++/java client。python只有写入测试工具，不推荐在生产使用，需要调用以下命令。其中swift python写入工具参数含义为：
    * --zk swift服务zk地址
    * --topic swift topic名，默认情况下一般等于表名
    * --count swift topic分片数，默认情况下一般等于表分片数
    * --schema 表schema地址
    * --file 要写入的havenask数据文件
```
## 查看swift服务的serviceZk地址
hape gs swift

## 写入测试消息
/ha3_install/swift_writer.py --zk <serviceZk> --topic in1 --count 2 --schema /ha3_install/example/cases/normal/in0_schema.json --file /ha3_install/example/data/rt.data 
```

## 六. 集群清理
```
## 清理havenask集群，删除容器、zk
hape delete havenask

## 清理swift集群，删除容器、zk
hape delete swift
```

## 七. 更多
* 更多集群子命令见[Hape子命令](HapeCmd-1.0.0.md)
* 更多运维场景见[Hape运维场景](Hape运维场景.md)
* 更多基于单机模式的例子见hape/example/README.md