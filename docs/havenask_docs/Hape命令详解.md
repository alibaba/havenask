# hape命令

* 运行以下命令可以获取hape的帮助

```
hape -h
```

* 子命令也可以获取帮助，例如

```
hape start -h

hape start havenask -h
```

# 子命令

## start 
* 作用：拉起集群
* 参数：
    * 传入havenask表示拉起havenask集群
    * 传入swift表示拉起swift集群
* 选项：
    * -c \<config\> 可选，表示指定hape配置，不传时默认为/ha3_install/hape_conf/default配置
    * -v 可选，表示打开hape debug级别日志
* 说明
    * 当参数选择havenask的时候
        * 如果发现swift集群不存在，hape会自动根据配置拉起
        * 如果发现指定集群配置中global.conf的domainZkRoot没有指定，hape会自动拉起默认的测试zookeeper
```
## 拉起swift集群
hape start swift

## 拉起havenask集群
hape start havenask

```


## stop
* 作用：停止集群，不清理集群zookeeper信息
* 参数：
    * havenask/swift/container 表示停止havenask集群/swift集群/某个容器
* 选项：
    * -c \<config\> 可选，表示指定hape配置，不传时默认为/ha3_install/hape_conf/default配置
    * -v 可选，表示打开hape debug级别日志
* 特殊选项：
    * 当参数为container的时候（这些信息可以从gs子命令中获得）
        * -i \<ip\> ip地址
        * -n \<name\> 对应ip机器上要停止的容器名 
```
## 停止swift集群
hape stop swift

## 停止havenask集群
hape stop havenask

## 停止某个机器上的一个容器
hape stop container -i <ip> -n <container-name>

```


## restart
* 作用：重启集群
* 参数：
    * havenask/swift/container 表示停止havenask集群/swift集群/某个容器
    * container 表示重启某个容器，具体用法见[Hape运维场景](Hape运维场景.md)
* 选项：
    * -c \<config\> 可选，表示指定hape配置，不传时默认为/ha3_install/hape_conf/default配置
    * -v 可选，表示打开hape debug级别日志
* 特殊选项：
    * 当参数为container时还有以下选项
        * -i \<ip\> 必选，机器ip
        * -n \<name\> 必选，容器名称
```
## 重启swift集群
hape restart swift

## 重启havenask集群
hape restart havenask

## 重启某个机器上的一个容器
hape restart container -i <ip> -n <container-name> 
```

## delete
* 作用：删除集群或表，并清理集群zookeeper信息
* 参数：
    * havenask/swift/table/all 表示删除特定类型目标
* 选项：
    * -c \<config\> 可选，表示指定hape配置，不传时默认为/ha3_install/hape_conf/default配置
    * -v 可选，表示打开hape debug级别日志
* 当参数为table时有以下选项
    * -t \<table\> 必选，表示删除某张特定表
    * --keeptopic 可选，表示删除表时是否保留swift topic
    
```
## 删除swift集群
hape delete swift

## 删除havenask集群
hape delete havenask

## 删除havenask表
hape delete table -t <table>

## 一次删除havenask、表、swift
hape delete all

```

## create
* 作用：创建表
* 参数：
    * table 表示建表
* 选项：
    * -c \<config\> 可选，表示指定hape配置，不传时默认为/ha3_install/hape_conf/default配置
    * -v 可选，表示打开hape debug级别日志
    * -t \<table\> 必选，表示表名字
    * -p \<partition-count\> 必选，表示分片数,单机下必须为1。多机模式下注意分片数不要超过可用于调度的searcher机器数，否则会导致端口冲突。另外多机模式下，假设有N张表，其中最大分片为M，那么所有表的分片要求必须要么是1，要么是M
    * -s \<schema\> 必选，表示schema路径
    * -f \<file\> 可选，表示全量文件路径，支持文件或目录。如果传入则创建的是全量表，不传则创建的是直写表。多机模式下必须传入hdfs路径
* 说明：
    * 在创建表时会自动创建swift topic。也可以提前用swift tools创建与表同名的topic，则建表时会复用
```
### 直写表
hape create table -t in0 -p 1 -s /ha3_install/example/cases/normal/in0_schema.json

### 全量表
hape create table -t in1 -p 1 -s /ha3_install/example/cases/normal/in0_schema.json  -f /ha3_install/example/cases/normal/test.data

```

## gs
* 作用：获取状态，详细含义见[Hape集群状态](Hape集群状态.md)
* 参数
    * swift/table/havenask/bs 获取swift/table/havenask/bs的状态
* 选项
    * -c \<config\> 可选，表示指定hape配置，不传时默认为/ha3_install/hape_conf/default配置
    * -v 可选，表示打开hape debug级别日志
    
* 特殊选项
    * 当参数为table时有以下选项
        * -t \<table-name\> 可选，用于只看某张表的状态

```
### 获取swift状态
hape gs swift

### 获取havenask集群状态
hape gs havenask

### 获取表状态
hape gs table
hape gs table -t in0

### 获取bs状态
hape gs bs

```


## validate
* 作用：对于集群做基本检查，如连通性、是否能完成容器调度等
* 选项
    * -c \<config\> 可选，表示指定hape配置，不传时默认为/ha3_install/hape_conf/default配置
    * -v 可选，表示打开hape debug级别日志

```
hape validate
```


## update
* 作用：更新hape部分配置，详细作用见[Hape运维场景](Hape运维场景.md)
* 参数：
    * candidate 根据global.conf更新集群可用于调度的ip列表
    * template 重启havenask admin以加载最新模板

* 选项
    * -c \<config\> 可选，表示指定hape配置，不传时默认为/ha3_install/hape_conf/default配置
    * -v 可选，表示打开hape debug级别日志

```
## 更新可用于调度的机器ip列表
hape update candidate
## 重启havenask admin以加载最新模板
hape update template
```

