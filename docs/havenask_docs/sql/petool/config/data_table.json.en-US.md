---
toc: content
order: 1
---
# data_table configuration
## Introduction
The $table_name_table.json file describes the details of how the raw data is processed, where $table_name is the name of the data table, which is generally the same as the index table. The configuration file consists of three parts: processor_chain_config, processor_config, and processor_rule_config. processor_chain_config configures the specific data processing logic, processor_config sets the number of threads and queue size for data processing, and processor_rule_config sets the number and concurrency of data processing nodes. Here is an example of a complete configuration file:
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
## Configuration details
### processor_chain_config
processor_chain_config is an array that can contain multiple processing chains. When multiple processing chains exist, it indicates that a piece of data is processed by different processing chains and used for index construction of multiple index tables. The following is the configuration of a processing chain.
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
* clusters: Configure the index building of which index tables are used for the documents produced by this processing chain.
* document_processor_chain: specifies the processing flow of the document_processor_chain. Set the following parameters for each processing node.
* class_name: The name of the processing node, which needs to be the same as the name set in the processing node implementation class.
* module_name: name of the dynamic library where the class of the processing node resides. If the built-in module_name of the processing node needs to be set to null, you can refer to the build_service/processor directory for supported processing nodes.
* parameters: A kv map, value must be a string, handle the node's parameters.
modules: Configures a dynamic library of plugins used in the processing chain. If the nodes in the processing chain are all built-in nodes, modules can be configured as an empty array. (<font color="red"> Note: The current version of the custom processor plug-in must be defined as a built-in plug-in. See the processor plug-in example. </font>)
* module_name: module name. The value can be any string and must be the same as module_name in the processing node.
* module_path: dynamic library path relative to the path of the plugins directory in the configuration directory.
* parameters: A kv map, value must be a string, dynamic library parameters.

### processor_config
```
{
    "processor_queue_size" : 2000,
    "processor_thread_num" : 10
}
```
* processor_thread_num: Number of threads to process the data. The more threads, the faster the data is processed when the cpu resources are sufficient.
* processor_queue_size: specifies the size of the data processing queue. Newly read data will wait in the queue to be processed. No new data will be read when the queue is full.

### processor_rule_config
```
{
    "parallel_num" : 1,
    "partition_count" : 1
}
```
* partition_count: indicates the number of data processing processes. You are advised to set this parameter to 1 during local index construction.
* parallel_num: parallel_num: concurrency of data processing when it is full. Number of data processing processes started when it is full =parallel_num * partition_count. You are advised to set the local index to 1.