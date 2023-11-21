---
toc: content
order: 1
---

# cluster configuration

The cluster configuration includes both online (havenask) configuration and offline (build_service) configuration.

The offline configuration includes various configurations of the builder and merger. The cluster configurations related to the builder and merger of build_service are as follows:
- build_option_config: indicates the configuration related to index sorting.
- offline_index_config: build related configuration. The default merge configuration. Configure periodic merge and full merge policies, and truncate them.
- swift_config, processed doc swift configuration, and builder swift reader configuration.
- cluster_config: specifies cluster-related configurations.
- realtime: true Enables the real-time function.

An example of a complete cluster configuration file in0_cluster.json is as follows:

```
{
    "build_option_config" : {
        "async_build" : true,
        "async_queue_size" : 1000,
        "document_filter" : true,
        "max_recover_time" : 30,
        "sort_build" : true,
        "sort_descriptions" : [
            {
                "sort_field" : "hits",
                "sort_pattern" : "asc"
            }
        ],
        "sort_queue_mem" : 4096,
        "sort_queue_size" : 10000000
    },
    "offline_index_config" : {
        "build_config" : {
            "build_total_memory" : 5120,
            "keep_version_count" : 40
        },
        "merge_config" :   
        {
            "merge_thread_count" : 3,
            "merge_strategy": "optimize",
            "merge_strategy_param": "",
            "document_reclaim_param" : "indexname=term1;indexname=term2",
            "uniq_pack_attribute_merge_buffer_size": 100,
            "truncate_strategy" : {...}
                
        },
        "customized_merge_config" :
        {
            "full":{
                "merge_config":{
                    "keep_version_count":40,
                    "merge_strategy":"optimize",
                    "merge_strategy_param":"after-merge-max-segment-count=20",
                    "merge_thread_count":4
                }
            },
            "inc"  :
            {
                "merge_config" : {
                    "merge_thread_count" : 5,
                    "merge_strategy": "optimize",
                    "merge_strategy_param": "",
                    "truncate_strategy" : {...}
                            
                },
                "period" : "period=1800",
                "merge_parallel_num" : 4,
                "need_wait_alter_field" : true
            },
            "merge-inc"  : {...}
               
        }
    },
    "cluster_config" : {
        "builder_rule_config" : {
            "batch_mode" : false,
            "build_parallel_num" : 1,
            "inc_build_parallel_num" : 1,
            "merge_parallel_num" : 1,
            "partition_count" : 1
        },
        "cluster_name" : "in0",
        "hash_mode" : {
            "hash_field" : "id",
            "hash_function" : "HASH"
        },
        "table_name" : "in0"
    },
    "swift_config" : {
        "topic_full": {
            "partition_count" : 10,
            "partition_min_buffer_size" : 10,
            "partition_max_buffer_size" : 1000,
            "partition_limit" : 10,
            "partition_file_buffer_size" : 100,
            "partition_resource" : 100,
            "obsolete_file_interval" : 3600,
            "reserved_file_count" : 10,
            "delete_topic_data" : true,
            "topic_mode" : "normal"
        },
        "topic_inc": {
            "partition_count" : 10,
            "partition_min_buffer_size" : 10,
            "partition_max_buffer_size" : 1000,
            "partition_limit" : 10,
            "partition_file_buffer_size" : 100,
            "partition_resource" : 100,
            "obsolete_file_interval" : 3600,
            "reserved_file_count" : 10,
            "delete_topic_data" : true,
            "topic_mode" : "normal"
        },        
        "reader_config" : "oneRequestReadCount=100;readBufferSize=10240;compress=true",
        "writer_config" : "functionChain=hashId2partId;mode=async|safe",
        "swift_client_config":"useFollowerAdmin=false;maxWriteBufferSizeMb=123;bufferBlockizeKb=124;mergeMessageThreadNum=22;tempWriteBufferPercent=0.23;refreshTime=122",
    },
    "direct_write" : true,  #开启直写模式
    "wal_config": { #直写模式下，wal相关配置
        "strategy": "realtime_swift",
        "sink": {
            "src": "swift",
            "type": "swift",
            "swift_root": "zfs://1.2.3.4:2181/xxx",
            "topic_name": "in0",
            "writer_config": "needTimestamp=true;functionChain=hashId2partId;compressThresholdInBytes=1024;mode=async|safe;maxBufferSize=2147483648;maxKeepInBufferTime=10000"
        },
        "timeout_ms": 10000
    },
    "realtime_info": {
        "need_field_filter": "true",
        "topic_name": "in0",
        "writer_config": "functionChain=hashId2partId;maxKeepInBufferTime=10000",
        "realtime_mode": "realtime_service_rawdoc_rt_build_mode",
        "src_signature": "1",
        "reader_config": "retryGetMsgInterval=50000;partitionStatusRefreshInterval=10000",
        "type": "swift",
        "swift_root": "zfs://1.2.3.4:2181/xxx"
    }  
}
```

### build_option_config
- sort_build: specifies whether the index is sorted. The default value is false.
- sort_descriptions: descriptions of the index are an array with each element in the array sorted by sort_field