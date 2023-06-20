  biz.json文件配置了查询节点启动时需要加载的表信息和一些集群的配置信息。biz.json的配置示例如下：

```
{
    "dependency_table": [
        "in0"
    ],
    "cluster_config" : {
        "hash_mode" : {
            "hash_field" : "docid",
            "hash_function" : "HASH"
        },
        "query_config" : {
            "default_index" : "title",
            "default_operator" : "AND"
        },
        "table_name" : "in0"
    }
}
```
* dependency_table：需要加载的表。
* cluster_config：一些集群相关的配置。
  * hash_mode：集群数据的hash方式，主要包括hash字段和使用的hash函数。
* query_config：query执行时的默认参数。
  * default_index：query中没有指定索引名时，默认查询的索引。
  * default_operator： 查询词分词后默认使用的连接符，可以是AND、OR。
  * table_name：默认查询的索引表名。

