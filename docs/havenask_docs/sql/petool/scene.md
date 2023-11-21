---
toc: content
order: 5
---

# Hape常见运维场景

## hape管理多个集群
* hape的所有命令都支持可选参数-c来制定集群配置路径
* 在配置中用global.conf的domainZkRoot和各个集群的serviceName实现不同的集群隔离，进而达成管理多个集群的目的
* 但是需要注意的是当前版本的havenask集群的admin、qrs、searcher端口是固定的，如果要管理多个集群最好设置为不同端口以防端口冲突

## 更新集群模板
* 集群配置下的cluster_templates代表集群模板。用户创建的集群和表会基于模板渲染产生实际的配置
* 用户更新模板后，需要调用以下子命令。由于集群模板由havenask admin管理，且当前版本下无法动态更新，因此需要调用以下命令，havenask admin会重启（worker不会）并重新加载最新的集群配置
```
## 基于模板更新已有集群配置
hape update template
```
* 当前版本不支持在模板内新增文件，只支持对于模板内已有文件如searcher_hippo.json的修改

## 更新表schema
* 1.1.0之前的版本不支持动态更新已有表，因此需要先删除表，然后再创建表
```
hape delete table <options-and-arguments>
hape create table <options-and-arguments>
```
* 1.1.0开始支持动态更新全量表schema，自动做全量并切换索引
```
hape update table-schema <options-and-arguments>
```

## 更新表配置
* 如果只是想要更新模板，使得对于新建的表生效，则只需调用
```
## 基于模板更新已有集群配置
hape update template
```
* 如果想要更新老表配置。当前版本暂时不支持动态更新已有表，因此需要删除表，更新模板、然后再创建表。此时database未必ready，需要用gs命令查看是否最终ready，见[Hape集群状态](status.md)
```
hape delete table <options-and-arguments>
hape update template
hape create table <options-and-arguments>
```

```
hape gs havenask
```

### 更新索引加载策略，在线索引配置
更新已经创建的表的在线配置，如实时build内存，索引加载策略等
```
hape update load-strategy <options-and-arguments>
```

## 集群资源&参数调整
集群资源和参数改调整本质上都是修改集群模板中对应的hippo.json
* 1.1.0之前需要重启集群havenask/swift
```
hape restart havenask/swift
```
* 1.1.0开始支持动态更新hippo.json
```
hape update hippo-config <options-and-arguments>
```
渲染后的默认hippo config内容可以用get命令获取:

```
hape get default-hippo-config -r searcher
hape get default-hippo-config -r qrs
```


## 集群状态查看
* 主要调用hape gs命令，见[Hape集群状态](status.md)



## 集群异常节点
* 当集群运行一段时间后可能有部分节点异常
* 普通可以通过重启解决的异常可以使用[Hape命令详解](command.md)中的hape restart container命令对于某个节点进行重启
* 无法通过重启解决的异常
    * 使用[Hape命令详解](command.md)中的hape stop container命令停止该节点
    * 在global.conf中去掉该ip，加上一个新ip
    * 使用[Hape命令详解](command.md)中的hape update candidate命令，过一段时间hippo调度会把应该属于当前机器上的进程分配到新机器上去
* 一般最后都要用hape gs命令确定havenask集群状态READY


## 修改分词器
* 直写表分词器定义：
    * cluster_templates/havenask/direct_table/analyzer.json
    * cluster_templates/havenask/direct_table/biz_config/analyzer.json
* 全量表分词器定义：
    * cluster_templates/havenask/offline_table/analyzer.json
* 可以参考模板中已经实现的jieba分词器。修改对应分词器定义文件后，调用以下命令即可
```
hape update template
```

