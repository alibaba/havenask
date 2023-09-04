# Overview
The $table_name_table.json file describes how the raw data is processed. $table_name is the name of the data table and is generally the same as that of the index table. The configuration file consists of three parts: processor_chain_config, processor_config, and processor_rule_config. processor_chain_config is used to configure the specific data processing logic, processor_config is used to configure the number of threads and queue size for data processing, and processor_rule_config is used to configure the number and concurrency of nodes for data processing. The following example describes a complete configuration file:
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
# Configuration details
## processor_chain_config
processor_chain_config is an array that can contain multiple processor chains. When multiple processor chains are involved, a set of data is processed by different processor chains and then used to build indexes of multiple index tables. The following is the configuration of a processor chain.
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
* clusters: used to configure the index table where the documents output by the processor chain are used to build indexes.
* document_processor_chain: used to configure the specific processing flow of the processor chain. The following parameters must be configured for each processing node:
   * class_name: specifies the name of the processing node, which must be the same as the name set in the implementation class of the processing node.
   * module_name: specifies the name of the dynamic library where the class of the processing node resides. If a processing node is built-in, leave this parameter unspecified. For more information about the processing nodes supported by the system, see the build_service/processor section.
   * parameters: specifies the parameters of the pocessing node in the format of a key value map. The value must be a string.
* modules: used to configure the dynamic library that stores plug-ins used in the processor chain. If all nodes in a processor chain are built-in nodes, this parameter can be configured as an empty array.
   * module_name: specifies the name of the module, which can be any string. It must be the same as the value of the module_name parameter in the processing node.
   * module_path: specifies the path of the dynamic library, relative to the path of the plugins directory in the configuration directory.
   * parameters: specifies the parameters of the dynamic library in the format of a key value map. The value must be a string.

## processor_config
```
{
    "processor_queue_size" : 2000,
    "processor_thread_num" : 10
}
```
* processor_thread_num: specifies the number of threads in data processing. When CPU resources are sufficient, the larger the number of threads, the faster the data processing.
* processor_queue_size: specifies the size of the data processing queue. The newly read data waits in the queue to be processed. When the queue is full, no new data is read.

## processor_rule_config
```
{
    "parallel_num" : 1,
    "partition_count" : 1
}
```
* partition_count: specifies the number of data processing processes. We recommend that you set this parameter to 1 when you build local indexes.
* parallel_num: specifies the concurrency of full data processing. The number of processes for full data processing = parallel_num x partition_count. We recommend that you set this parameter to 1 when you build local indexes.