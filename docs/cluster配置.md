# cluster配置

cluster配置中同时包含在线部分 (ha3) 配置和离线 (build_service) 两部分配置；

离线配置中包含了对builder和merger的各种配置，与build_service的builder和merger相关的cluster 配置主要有：
- build_option_config，索引排序相关配置。
- offline_index_config，build相关配置，缺省merge配置。定期 merge 及全量 merge 策略配置，截断配置。
- swift_config，processed doc swift 配置，以及 builder 的 swift reader 配置。
- cluster_config, cluster相关配置。
- slow_node_detect_config, 针对cluster级别的慢节点检测配置
- realtime: true表示开启实时功能，[实时功能使用文档](https://github.com/alibaba/havenask/wiki/%E5%AE%9E%E6%97%B6%E5%8A%9F%E8%83%BD%E4%BD%BF%E7%94%A8%E6%96%87%E6%A1%A3)

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
    "cluster_config" : {
        "builder_rule_config" : {
            "batch_mode" : false,
            "build_parallel_num" : 1,
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
    "offline_index_config" : {
        "build_config" : {
            "build_total_memory" : 5120,
            "keep_version_count" : 40
        }
    }
}
```

## build_option_config
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

## offline_index_options
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

## cluster_config
- hash_mode
    - hash_filed: 用于计算hash值的字段名
    - hash_function: hash函数，可填HASH/HASH64/GALAXY_HASH/NUMBER_HASH/KINGSO_HASH
- builder_rule_config
    - partition_count: 索引需要build成多少个partition。
    - build_parallel_num: 全量build时每个partition的并行进程数，对增量不生效。
    - inc_build_parallel_num: 增量build时每个partition的并行进程数，对全量不生效。
    - merge_parallel_num: 默认的merge并行数（老的配置方式），如果在offline_index_config中配置了，会覆盖此处的配置。
    - batch_mode: build service模式下，增量builder是否常驻，如果为true，则每次定期merge时，启动build。
    - map_reduce_ratio: job模式下，map与reduce个数的比例。
    - need_partition: job模式下，是否需要引擎分partition。
    - align_version: 开启align version 模式后，所有partition 的merge 过程将使用相同的versionId; 默认值是 true
