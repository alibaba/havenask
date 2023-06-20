# 简介
  $table_name_table.json文件描述原始数据如何处理的详细信息，其中$table_name是数据表的名称，一般和索引表相同。配置文件由三个部分组成：processor_chain_config、processor_config、processor_rule_config。其中processor_chain_config用于配置具体的数据处理逻辑，processor_config设置数据处理的线程数和队列大小，processor_rule_config设置数据处理节点的个数和并发度。下面是一个完整配置文件的示例：
```
{
    "processor_chain_config" : [
        {
            "clusters" : [
                "in0"
            ],
            "document_processor_chain" : [
                {
                    "class_name" : "TokenizeDocumentProcessor",
                    "module_name" : "",
                    "parameters" : {
                    }
                }
            ],
            "modules" : [
            ]
        }
    ],
    "processor_config" : {
        "processor_queue_size" : 2000,
        "processor_thread_num" : 10
    },
    "processor_rule_config" : {
        "parallel_num" : 1,
        "partition_count" : 1
    }
}
```
# 配置详解
## processor_chain_config
  processor_chain_config是一个数组，可以包含多个处理链，当存在多个处理链时，表示一份数据经过不同的处理链处理之后，用于多个索引表的索引构建，下面是一个处理链的配置。
```
        {
            "clusters" : [
                "in0"
            ],
            "document_processor_chain" : [
                {
                    "class_name" : "AnyClassName",
                    "module_name" : "example_module",
                    "parameters" : {
                        "param1" : "value1"
                    }
                },
                {
                    "class_name" : "TokenizeDocumentProcessor",
                    "module_name" : "",
                    "parameters" : {
                    }
                }
            ],
            "modules" : [
                {   
                    "module_name" : "example_module",
                    "module_path" : "libExample.so",
                    "parameters" : {
                    }
                }
            ]
        }
```
* clusters： 配置该处理链产出的文档用于哪些索引表的索引构建。
* document_processor_chain： 配置该处理链具体的处理流程，每个处理处理节点需要配置下面的参数。
  * class_name：处理节点的名称，需要与处理节点实现类中设置的名称相同。
  * module_name：处理节点的类所在的动态库名称，如果处理节点是内置的module_name需要设置为空，系统支持的处理节点可以参考build_service/processor目录。
  * parameters：一个kv map，value必须为字符串，处理节点的参数。
* modules：配置处理链中使用的插件的动态库。如果处理链中的节点都是内置节点，modules可以配置为空数组。
  * module_name：module的名称，可以是任意字符串，需要和处理节点中的module_name相同。
  * module_path：动态库路径，相对于配置目录中plugins目录的路径。
  * parameters：一个kv map，value必须为字符串，动态库的参数。

## processor_config
```
{
    "processor_queue_size" : 2000,
    "processor_thread_num" : 10
}
```
* processor_thread_num：数据处理的线程数，cpu资源充足的情况下，线程数越多数据处理越快。
* processor_queue_size：数据处理队列大小，新读取的数据会在队列中等待被处理，队列满时不会读取新数据。

## processor_rule_config
```
{
    "parallel_num" : 1,
    "partition_count" : 1
}
```
* partition_count：数据处理进程的个数，本地索引构建时建议配置为1。
* parallel_num：全量时数据处理的并发度，全量时启动的数据处理进程个数=parallel_num * partition_count。本地索引构建时建议配置为1。