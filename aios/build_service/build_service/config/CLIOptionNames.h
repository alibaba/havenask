#ifndef ISEARCH_BS_CLIOPTIONNAMES_H
#define ISEARCH_BS_CLIOPTIONNAMES_H


namespace build_service {
namespace config {

// constant
static const std::string KV_TRUE = "true";
static const std::string KV_FALSE = "false";

// help
static const std::string HELP = "help";

// common
static const std::string CONFIG_PATH = "config"; // required
static const std::string COUNTER_ZK_ROOT = "counter_zk_root"; // required
static const std::string RANGE_FROM = "range_from";
static const std::string RANGE_TO = "range_to";
static const std::string ZOOKEEPER_ROOT =  "zookeeper_root";
static const std::string COUNTER_CONFIG_JSON_STR =  "counter_config_jsonstr";

// monitor
static const std::string USER_NAME = "user_name";
static const std::string SERVICE_NAME = "service_name";
static const std::string AMON_PORT = "amon_agent_port";

// worker
static const std::string ALOG_CONF = "alog_conf";
static const std::string WORKDIR = "workdir";
static const std::string ROLE = "role";
static const std::string WORKER_PATH_VERSION = "worker_path_version";

// realtime
static const std::string REALTIME_MODE = "realtime_mode";
static const std::string REALTIME_SERVICE_MODE = "realtime_service_mode";
static const std::string REALTIME_JOB_MODE = "realtime_job_mode";
static const std::string REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE = "realtime_service_rawdoc_rt_build_mode";
static const std::string REALTIME_PROCESS_RAWDOC = "realtime_process_rawdoc";

// reader
static const std::string DATA_PATH = "data"; // required
static const std::string READ_SRC = "src"; // required
static const std::string READ_SRC_TYPE = "type";
static const std::string READ_SRC_MODULE_NAME = "module_name";
static const std::string READ_SRC_MODULE_PATH = "module_path";
static const std::string SWIFT_SRC_FULL_BUILD_LATENCY = "swift_source_full_build_latency";
static const std::string SWIFT_SRC_FULL_BUILD_TIMEOUT = "swift_source_full_build_time_limit";
static const std::string CUSTOMIZE_FULL_PROCESSOR_COUNT = "full_processor_count";
static const std::string RAW_DOCUMENT_SCHEMA_NAME = "raw_doc_schema_name";

static const std::string FILE_READ_SRC = "file";
static const std::string FIX_LEN_BINARY_FILE_READ_SRC = "fix_length_binary_file";
static const std::string VAR_LEN_BINARY_FILE_READ_SRC = "var_length_binary_file";
static const std::string SWIFT_READ_SRC = "swift";
static const std::string PLUGIN_READ_SRC = "plugin";
static const std::string READ_SRC_SEPARATOR = ",";

static const std::string INDEX_CLONE_SRC = "index_clone";
static const std::string SOURCE_INDEX_ADMIN_ADDRESS = "source_admin_address";
static const std::string SOURCE_INDEX_BUILD_ID = "source_build_id";
static const std::string MADROX_ADMIN_ADDRESS = "madrox_admin_address";
static const std::string INDEX_CLONE_LOCATOR = "cloned-index-locator";


static const std::string EXTEND_FIELD_NAME = "extend_field_name";
static const std::string EXTEND_FIELD_VALUE = "extend_field_value";
static const std::string BS_RAW_FILES_META = "bs_raw_file_meta";
static const std::string BS_RAW_FILE_META_DIR = "bs_raw_files_meta_dir";

static const std::string RAW_DOCUMENT_FILE_PATTERN = "file_pattern";
static const std::string TRACE_FIELD = "trace_field";
static const std::string TIMESTAMP_FIELD = "timestamp_field";
static const std::string SOURCE_FIELD = "source_field";
static const std::string SOURCE_DEFAULT = "source_default";
static const std::string SRC_SIGNATURE = "src_signature";
static const std::string NEED_FIELD_FILTER = "need_field_filter";
static const std::string HA_RESERVED_SOURCE = "ha_reserved_source";

static const std::string SWIFT_ZOOKEEPER_ROOT = "swift_root";
static const std::string SWIFT_TOPIC_NAME = "topic_name";
static const std::string SWIFT_READER_CONFIG = "reader_config";
static const std::string SWIFT_WRITER_CONFIG = "writer_config";
static const std::string SWIFT_CLIENT_CONFIG = "swift_client_config";
static const std::string DEFAULT_SWIFT_BROKER_TOPIC_CONFIG = "topic";
static const std::string FULL_SWIFT_BROKER_TOPIC_CONFIG = "topic_full";
static const std::string INC_SWIFT_BROKER_TOPIC_CONFIG = "topic_inc";

// worker common
static const std::string RESERVED_CLUSTER_CHECKPOINT_LIST = "reserved_checkpoint_list";
static const std::string RESERVED_VERSION_LIST = "reserved_version_list";

// processor
static const std::string DATA_TABLE_NAME = "data_table";
static const std::string DATA_DESCRIPTION_KEY = "data_description";
static const std::string DATA_DESCRIPTION_PARSER_CONFIG = "parser_config";

// region config
static const std::string DEFAULT_REGION_FIELD_NAME = "ha_reserved_region_field";
static const std::string REGION_DISPATCHER_CONFIG = "dispatcher_type";
static const std::string FIELD_DISPATCHER_TYPE = "dispatch_by_field";
static const std::string REGION_FIELD_NAME_CONFIG = "region_field_name";
static const std::string REGION_ID_DISPATCHER_TYPE = "dispatch_by_id";
static const std::string REGION_ID_CONFIG = "region_id";
static const std::string ALL_REGION_DISPATCHER_TYPE = "dispatch_to_all_region";

// processor::swift
static const std::string PROCESSED_DOC_SWIFT_ROOT = "processed_doc_swift_root";
static const std::string PROCESSED_DOC_SWIFT_TOPIC_PREFIX = "processed_swift_topic_prefix";
static const std::string PROCESSED_DOC_SWIFT_READER_CONFIG = "processed_swift_reader_config";
static const std::string PROCESSED_DOC_SWIFT_WRITER_CONFIG = "processed_swift_writer_config";
static const std::string CHECKPOINT_ROOT = "checkpoint_root";
static const std::string CHECKPOINT = "checkpoint";
static const std::string SWIFT_START_TIMESTAMP = "swift_start_timestamp";
static const std::string SWIFT_STOP_TIMESTAMP = "swift_stop_timestamp";
static const std::string SWIFT_SUSPEND_TIMESTAMP = "swift_suspend_timestamp";

// builder
static const std::string INDEX_ROOT_PATH = "index_root"; // required
static const std::string SHARED_TOPIC_CLUSTER_NAME = "shared_topic_cluster_name";
static const std::string BUILDER_INDEX_ROOT_PATH = "builder_index_root";
static const std::string CLUSTER_NAMES = "cluster_names"; // required
static const std::string GENERATION_ID = "generation";
static const std::string CLUSTER_NAME_SEPARATOR = ",";
static const std::string CLUSTER_INFO_SEPARATOR = ":";
static const std::string BUILD_MODE = "build_mode";
static const std::string BUILD_MODE_FULL = "full";
static const std::string BUILD_MODE_INCREMENTAL = "incremental";
static const std::string BUILD_MODE_DEFAULT = BUILD_MODE_FULL;
static const std::string PARTITION_COUNT = "partition_count";
static const std::string BUILD_PARTITION_FROM = "build_partition_from";
static const std::string BUILD_PARTITION_COUNT = "build_partition_count";
static const std::string PROCESSED_DOC_SWIFT_STOP_TIMESTAMP = "processed_doc_swift_stop_timestamp";
static const std::string OPERATION_IDS = "operation_ids";


// job
static const std::string BUILD_PARALLEL_NUM = "build_parallel_num";
static const std::string MERGE_PARALLEL_NUM = "merge_parallel_num";
static const std::string MAP_REDUCE_RATIO = "map_reduce_ratio";
static const std::string PART_SPLIT_NUM = "partition_split_num";
static const std::string NEED_PARTITION = "need_partition";
static const std::string MERGE_CONFIG_NAME = "merge_config_name";
static const std::string MERGE_TIMESTAMP = "merge_timestamp";
static const std::string ALIGNED_VERSION_ID = "aligned_versionid";
static const std::string TARGET_DESCRIPTION = "target_description";
}
}

#endif //ISEARCH_BS_CLIOPTIONNAMES_H
