## Cluster configuration



The cluster configuration includes online (HHavenask V3) configuration and offline (build_service) configuration.



Offline configuration contains various configurations for builders and mergers. Cluster configurations related to builders and merges of build_service mainly include:

- build_option_config: specifies the configuration related to index sorting.

- offline_index_config: specifies the build configuration and default merge configuration. Valid values include periodic merge and full merge policy configuration, which are truncation configurations.

- swift_config: specifies the processed doc swift configuration, and the swift reader configuration of the builder.

- cluster_config: specifies the cluster-related configuration.

- slow_node_detect_config: specifies the configuration for detecting cluster-level slow nodes.



The following example shows a complete cluster configuration file named in0_cluster.json:

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



### build_option_config

- sort_build : specifies whether the indexes are sorted. The default value is false.

- sort_descriptions : specifies the sorting method of the index. The value is an array, where each element consists of the values of sort_field and sort_pattern. Multi-dimensional sorting is available. When the sorting fields in the first dimension are the same, results are sorted according to the second-dimensional description. The rest can be done in the same manner

- sort_field : specifies the field in the schema configuration. Only numeric fields such as INT, HASH, DOUBLE, and FLOAT are allowed. The sorting field cannot be updated and the update message does not take effect for this field.

- sort_pattern : specifies the sorting method. Valid values include asc and desc. asc indicates the sorting is in ascending order and desc indicates the sorting is in descending order. The default value is desc.

- sort_queue_size : specifies the size of segment generated during the build process. The document is sorted only after the segment size reaches this limit in the memory and then an index is generated. The default value is uint32_t::max().

- sort_queue_mem: The unit is MB. By default, the value is 60% of the value of build_total_memory. The size of the sorting queue is the smaller value of the sort_queue_mem and sort_queue_size.

- document_filter: specifies whether to enable document filtering. The default value is true. When this feature is disabled, the incremental stream document is the same as the real-time stream document. This way, the Havenask 3 reopen performance can be improved.

- async_build: specifies whether ProcessedDoc deserialization and the build process are asynchronous and each occupies one thread. The default value is false.

- async_queue_size : specifies the size of the intermediate queue in the case of asynchronous build. The default value is 1000.

- max_recover_time: specifies the maximum duration used for tracking real-time messages after the index is fully loaded, in seconds.

- recover_delay_tolerance_ms: specifies whether real-time messages are tracked after the index is fully loaded, in milliseconds. The default value is 1000 ms. If the difference between the latest locator of swift and the latest locator in the current engine is less than the value, real-time messages are tracked.

- build_qps_limit: specifies the limit of real-time build, in QPS.



### offline_index_options

offline_index_options configuration is divided into three parts: build_config, merge_config, and customized_merge_config.



- build_config: specifies the offline build configuration.

   - build_total_memory: specifies the total memory used to build the index. The unit is MB. The default value is 6 GB. The value of build_total_memory can be specified by the environment variable BUILD_TOTAL_MEM. The value of build_total_memory specified by you prevails on the value specified by the environment variable BUILD_TOTAL_MEM. The value specified by the environment variable BUILD_TOTAL_MEM prevails on the default value.

   - max_doc_count : specifies the maximum number of documents that triggers dump when you build the index. The default value is max_uint32.

   - keep_version_count: specifies the number of versions retained after you build the index. It mainly controls the number of offline versions of incremental builds and local real-time versions. Excessive versions are deleted. The default value is 2.

   - enable_package_file: specifies whether to enable the package file feature. If it is enabled, each segment of the index is packaged as a package file. This feature is able to reduce the pressure on HDFS.

   - hash_map_init_size: specifies the initial size of the hash map used when you build the inverted index.

   - dump_thread_count: specifies the number of threads used by the dump index.

   - speedup_primary_key_reader: specifies whether to accelerate data read based on the primary key.

- merge_config: specifies the default merge configuration.

   - merge_thread_count: specifies the number of parallel threads used by the merge process.

   - merge_strategy: specifies the merge policy of the index.

   - merge_strategy_param: specifies the parameter of the index merge policy. For more information, see the merge policy and its parameters below.

   - uniq_pack_attribute_merge_buffer_size: specifies the size of the buffer used by the pack attribute during the merge process. The default value is 64 MB. This parameter takes effect only when the schema is configured with a pack attribute that has multi-value attribute deduplication enabled.

   - document_reclaim_param: specifies the conditions under which the merge process reclaims documents. Multiple conditions are associated by using the OR operator. All segments are reclaimed, including but not limited to segments that join the merge.

   - truncate_strategy: specifies the truncation policy.

- customized_merge_config: specifies the user-defined configuration. You can customize different merge policies based on different scenarios.



### cluster_config

- hash_mode

   - hash_filed: specifies the name of the field used to calculate the hash value.

   - hash_function: specifies the hash function. Valid values: HASH, HASH64, GALAXY_HASH, NUMBER_HASH, and KINGSO_HASH.

- builder_rule_config

   - partition_count: specifies the number of partitions on which the index is built.

   - build_parallel_num: specifies the number of parallel processes per partition when you build full indexes. This parameter does not take effect for incremental index building.

   - inc_build_parallel_num: specifies the number of parallel processes in each partition when you build incremental indexes. This parameter does not take effect for full index building.

   - merge_parallel_num: specifies the number of merge rows. This is an old configuration method. Configuration of the offline_index_config parameter prevails over the configuration of this parameter.

   - batch_mode: specifies whether the incremental builder resides in the build service mode. If this parameter is set to true, the build process is started each time you periodically merge clusters.

   - map_reduce_ratio: specifies the ratio of the number of maps to the number of reduces in job mode.

   - need_partition: specifies whether partition is required by the engine in job mode.

   - align_version: When this feature is enabled,  all partitions use the same version ID during the merge process. The default value is true.