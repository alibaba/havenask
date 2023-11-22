---
toc: content
order: 1
---

# cluster配置

cluster配置中同时包含在线部分（havenask）配置和离线 (build_service) 两部分配置；

离线配置中包含了对builder和merger的各种配置，与build_service的builder和merger相关的cluster 配置主要有：
- build_option_config，索引排序相关配置。
- offline_index_config，build相关配置，缺省merge配置。定期 merge 及全量 merge 策略配置，截断配置。
- swift_config，processed doc swift 配置，以及 builder 的 swift reader 配置。
- cluster_config, cluster相关配置。
- realtime: true表示开启实时功能。

一个完整的cluster配置文件in0_cluster.json示例如下：
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
- sort_build : 索引是否排序，默认为 false。
- sort_descriptions : 索引的排序方式，是一个数组，数组中每个元素由排序字段 sort_field 与排序方式 sort_pattern 组成。允许多维排序；若第一维排序字段相同，则会按照第二维描述排序，依次类推。
- sort_field ： schema 配置中的字段，只配置允许 INT，HASH，DOUBLE，FLOAT 等数值类型字段，但是不支持对sort字段update（update消息对该字段不生效）。
- sort_pattern ： 排序方式，有 升序 asc 和 降序 desc 两种选择，默认为 desc。
- sort_queue_size ：build 过程中每次产生 segment 大小，文档在内存中积累到这个数量之后才会排序，并且生成出索引，默认uint32_t::max()。
- sort_queue_mem：单位MB，默认为build_total_memory的60%，sort队列大小取sort_queue_mem和sort_queue_size的最小。
- document_filter ：是否允许文档过滤（关闭该功能，表示增量流与实时流文档一致，可以提高HA3 ReOpen性能），默认值为true。
- async_build ：ProcessedDoc反序列化与Build是否异步（各占一个线程），默认为false。
- async_queue_size ：异步Build情况下，中间队列大小，默认为1000。
- max_recover_time：单位为秒，全量加载索引后追实时消息，最多花费这么长的时间。
- recover_delay_tolerance_ms：单位毫秒，全量加载索引后追实时消息，当swift最新消息locator和目前引擎中最新locator相差小于此时间时，视为追上实时，默认值为1000ms。
- build_qps_limit: 单位为qps，Realtime Build 限速。

### offline_index_options
offline_index_options的配置分为build_config， merge_config 和 customized_merge_config 三部分：

- build_config: 离线build配置。
    - build_total_memory :  build索引时所使用的总内存。单位是M。默认是 6GB。build_total_memory支持通过环境变量"BUILD_TOTAL_MEM"赋值，优先级顺序为：用户配置>环境变量>本身默认值
    - max_doc_count :  build索引时doc数超过该限制时，会触发Dump, 本身默认值为max_uint32.
    - keep_version_count: build 索引后，保留下来的版本个数。主要针对增量 build 的离线版本和实时 build 的本地实时版本个数进行控制，多余的版本会被删掉。默认是2。
    - enable_package_file: 是否启用package file 功能。如果启用，索引的每个segment会被打包为一个package文件。该功能用于减小对HDFS的压力。
    - hash_map_init_size: 指定构建倒排时使用的hash_map的初始大小
    - dump_thread_count: dump 索引使用的线程个数
    - speedup_primary_key_reader: 是否启用主键读取加速
- merge_config: 缺省merge配置。
    - merge_thread_count: merge 过程使用的并行线程数量。
    - merge_strategy: 索引merge策略。
    - merge_strategy_param: 索引的merge策略参数，具体配置见下文的merge策略及其参数介绍。
    - uniq_pack_attribute_merge_buffer_size: 该参数仅在schema配置有带uniq压缩的pack attriubte的情况下生效。默认为64MB。用来控制pack attribute 在merge过程中使用的buffer大小。
    - document_reclaim_param: 指定merge过程回收doc的条件，多个条件之间为or关系；对所有segment均进行回收，不限于参加merge的segment
    - truncate_strategy: 截断策略
- customized_merge_config: 用户自定义配置。用户可以对不同场景定制不同的merge策略

### cluster_config
- hash_mode
    - hash_filed: 用于计算hash值的字段名
    - hash_function: hash函数，可填HASH/HASH64/GALAXY_HASH/NUMBER_HASH/KINGSO_HASH
- builder_rule_config
    - partition_count: 索引需要build成多少个partition。
    - build_parallel_num: 全量build时每个partition的并行进程数，对增量不生效。
    - inc_build_parallel_num: 增量build时每个partition的并行进程数，对全量不生效。
    - merge_parallel_num: 默认的merge并行数（老的配置方式），如果在offline_index_config中配置了，会覆盖此处的配置。
    - batch_mode: build service模式下，增量builder是否常驻，如果为true，则每次定期merge时，启动build。
### swift_config
swift_config是用于配置对中转topic（用于转发处里过的文档）的读写相关参数，可以为空，线上可以通过此项配置优化读写swift的性能。该配置只在非只写模式下生效，直写模式可以不用配置。
- topic_full和topic_inc，如果全量和实时使用不同的中转topic，需要分别配置"topic_full"和"topic_inc"，否则只需要配置"topic"即可。
  -- partition_count，中转topic的分片，分片越大可以支持的数据更新越高。
  -- partition_min_buffer_size，每个分片最小缓存大小，单位MB。
  -- partition_max_buffer_size，每个分片最大缓存大小，单位MB。
  -- partition_limit，一个swift节点可以加载的最大分片数。
  -- partition_file_buffer_size，读取缓存的文件时buffer的大小。
  -- partition_resource，一个分片占用的资源数，可以防止一个swift节点加载过多的分片。
  -- obsolete_file_interval，数据过期时间，单位小时。
  -- reserved_file_count" : 缓存的文件数，中转消息缓存的文件个数大于该值，且已经超过了过期时间，文件会被清理。
  -- delete_topic_data，删除topic时清理对应的topic数据。
  -- topic_mode: topic模式，可以为security、memory_prefer、normal，默认为normal。
        -- security，同步写数据，数据写入时需要同步落盘才返回，数据安全但是会影响写入性能。
        -- memory_perfer，数据一般不落盘，只有当下游超时没有消费时才落盘，只有在topci_full中生效，不建议生成使用。
        -- normal，数据定期落盘。
### 直写模式相关配置
* direct_write，true或者flase，为true时表示开启直接模式。
#### wal_config，直接模式下，wal相关配置
* strategy，wal策略，建议为realtime_swift，表示使用swift进行wal。
*  sink
  -- src，设置为swift。
  -- type，设置为swift。
  -- swift_root，swift的zk地址。
  -- topic_name，用于wal的swift的topic。
  -- writer_config，wal时写swift的参数。
#### realtime_info，直接模式下searcher读取实时数据的配置
* topic_name，用于wal的topic，与wal_config中的topic_name一致。
* swift_root，swift的zk地址，与wal_config中的swift_root一致。
* reader_config，读取swift的相关配置。
* realtime_mode，设置为realtime_service_rawdoc_rt_build_mode。