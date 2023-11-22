---
toc: content
order: 1
---
 
# 引擎配置介绍
## 简介
* 对于用户建立的每一张表，Hape都会根据用户配置的[Hape配置](hape-config.md)中的全局和调度信息，渲染一份表配置下发到引擎里
* Hape的表配置模板位于hape_conf的direct_tables（直写表模板）或offline_tables（全量表模板）

<Tree>
  <ul> 
    <li>default
      <ul>
        <ul>
        <li>global.conf</li> 
        <li>cluster_templates
            <ul>
            <li>havenask (havenask引擎进程配置)
                <ul><li>direct_tables（直写表模板）/offline_tables（全量表模板）
                </li></ul>
                <ul><li>clusters <ul><li>...</li></ul>
                </li></ul>
                <ul><li>data_tables <ul><li>...</li></ul>
                </li></ul>
                <ul><li>analyzer.json</li></ul></ul></li>
            </li>
            <li>swift
                <ul><li>...</li></ul>
            </li>
            </ul>
        </li> 
        </ul>
    </li>
    </ul>
  </ul>
</Tree>

## clusters
  clusters是一个必选的目录，里面存储一个和多个索引表构建时的具体参数。配置文件是一个json格式的文件，文件名由索引表名称和后缀“_cluster.json“组成。索引构建配置的具体格式请参考[cluster配置](./clusterconfig)。

## data_tables
  data_tables是一个必须的目录，里面存储一个或多个表数据处理配置，用于指导引擎在索引构建时如何对原始数据进行处理。数据处理配置文件是一个json格式的文件，文件名由索引表名称和后缀“_table.json”组成。数据处理配置文件的具体格式请参考[data_table配置](./data_table.json)。


## schemas
  schemas是在用户调用Hape建表时指定的，里面存储一个或多个表的索引结构配置。索引结构配置文件是一个json格式的文件，文件名由索引表名称和后缀”_schema.json“组成。索引结构配置文件的具体格式请参考[schema配置](./schemaconfig)。

## analyzer.json
  analyzer.json是一个分析器配置文件，配置文件中指定了引擎所有可以使用的分析器，具体格式请参考[分词器配置](./analyzer)。

