# Hape 单机模式

* 单机模式指在一台机器上拉起havenask集群的多个容器，主要用于试验
* 以下任何操作出现异常可以参考[Hape常见问题与排查](Hape常见问题与排查.md)进行排查


##  一. 准备环境和配置

### 准备环境
* 最新的镜像为registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:latest，不同版本的镜像不同，可以参考[release页面](https://github.com/alibaba/havenask/releases)获取镜像版本，需要确保已经用docker拉取对应版本的镜像。
* 确保本机能免密ssh登录自己，以下命令打通免密登录自己:
```
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
ssh-copy-id -i ~/.ssh/id_rsa `hostname -i`
```

### 修改配置
* 在默认情况下hape采用/ha3_install/hape_conf/default作为集群配置，无需修改
* 配置的具体各项含义可以参考[Hape配置详解](Hape配置详解.md)文档


## 二、创建havenask集群

*  创建并进入hape容器
```
./docker/havenask/create_container.sh <container_name> <image>
./<container_name>/sshme
```

* （可选）验证hape运行环境无明显异常
```
hape validate
```

* 创建havenask集群。所有后续进程的容器都在当前机器被拉起
```
hape start havenask
```

## 三. 表管理
* havenask表分为直写表和全量表两种，可以选择其中一种进行创建，或两种都创建
* 创建直写表
    * -t表示表名字
    * -p表示分片数,单机下必须为1
    * -s表示schema路径
```
hape create table -t in0 -p 1 -s /ha3_install/example/cases/normal/in0_schema.json

```
* (启动速度慢，可选) 创建全量表
    * -t表示表名字
    * -p表示分片数,单机下必须为1
    * -s表示schema路径
    * -f表示全量文件路径，支持文件或目录
```
hape create table -t in1 -p 1 -s /ha3_install/example/cases/normal/in0_schema.json  -f /ha3_install/example/data/test.data
```
* 由于全量表需要拉起bs服务，启动多个bs任务，一般需要等待3~5分钟生效，因此可以用以下命令查看bs状态。状态含义见[Hape集群状态](Hape集群状态.md)
```
hape gs bs

## 当带有full进程都结束，只剩下inc进程时，执行以下命令发现对应表状态为READY
hape gs table -t <table_name>
```

* 如果想要删除表，可以执行以下操作
```
hape delete table -t <table-name>
```

## 四. 查看集群状态
* 在完成建表后，可以使用以下命令查看集群状态，判断集群是否已经准备好可以读写。状态含义见[Hape集群状态](Hape集群状态.md)
```
## 当返回的结果中，sqlClusterInfo的clusterStatus为READY时集群已经准备好
hape gs havenask
```

## 五. 读写数据
* 直写表写入数据命令如下
```
/ha3_install/sql_query.py --query "insert into in0 (createtime,hits,id,title,subject)values(1,2,4,'测试', '测试')"
```

* 查询表数据
```
/ha3_install/sql_query.py --query "select * from in0"
```

* 如果想要查看text类型字段值，则需要联合查询[摘要索引](摘要索引.md)，命令如下
```
/ha3_install/sql_query.py --query "select in0_summary_.id, in0_summary_.title, in0_summary_.subject from in0 inner join in0_summary_ on in0.id=in0_summary_.id"
```



* 全量表写实时数据推送需要swift。目前推荐使用swift c++/java client。python只有写入测试工具，不推荐在生产使用，需要调用以下命令。其中swift python写入工具参数含义为：
    * --zk swift服务zk地址
    * --topic swift topic名，默认情况下一般等于表名
    * --count swift topic分片数，默认情况下一般等于表分片数
    * --schema 表schema地址
    * --file 要写入的havenask数据文件
```
## 查看swift服务的serviceZk地址，默认配置下是zfs://<localhost>:2181/havenask/havenask-swift-local
hape gs swift

## 写入测试消息
/ha3_install/swift_writer.py --zk <serviceZk> --topic in1 --count 1 --schema /ha3_install/example/cases/normal/in0_schema.json --file /ha3_install/example/data/rt.data
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
