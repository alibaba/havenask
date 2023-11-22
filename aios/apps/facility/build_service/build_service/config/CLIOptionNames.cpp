/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "build_service/config/CLIOptionNames.h"

namespace build_service { namespace config {
// constant
const std::string KV_TRUE = "true";
const std::string KV_FALSE = "false";

// help
const std::string HELP = "help";

// common
const std::string CONFIG_PATH = "config";              // required
const std::string COUNTER_ZK_ROOT = "counter_zk_root"; // required
const std::string RANGE_FROM = "range_from";
const std::string RANGE_TO = "range_to";
const std::string ZOOKEEPER_ROOT = "zookeeper_root";
const std::string COUNTER_CONFIG_JSON_STR = "counter_config_jsonstr";
const std::string SAFE_WRITE = "safe_write";

// monitor
const std::string USER_NAME = "user_name";
const std::string SERVICE_NAME = "service_name";
const std::string AMON_PORT = "amon_agent_port";

// worker
const std::string ALOG_CONF = "alog_conf";
const std::string WORKDIR = "workdir";
const std::string ROLE = "role";
const std::string WORKER_PATH_VERSION = "worker_path_version";

// realtime
const std::string REALTIME_MODE = "realtime_mode";
const std::string BS_SERVER_ADDRESS = "bs_server_address";
const std::string APP_NAME = "app_name";
const std::string REALTIME_SERVICE_MODE = "realtime_service_mode";
const std::string REALTIME_JOB_MODE = "realtime_job_mode";
const std::string REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE = "realtime_service_rawdoc_rt_build_mode";
const std::string REALTIME_SERVICE_NPC_MODE = "realtime_service_npc_mode";

// reader
const std::string DATA_PATH = "data"; // required
const std::string READ_SRC = "src";   // required
const std::string READ_SRC_CONFIG = "read_src_config";
const std::string READ_SRC_TYPE = "type";
const std::string READ_STREAMING_SOURCE = "streaming_source";
const std::string READ_SRC_MODULE_NAME = "module_name";
const std::string READ_SRC_MODULE_PATH = "module_path";
const std::string SWIFT_SRC_FULL_BUILD_LATENCY = "swift_source_full_build_latency";
const std::string SWIFT_SRC_FULL_BUILD_TIMEOUT = "swift_source_full_build_time_limit";
const std::string CUSTOMIZE_FULL_PROCESSOR_COUNT = "full_processor_count";
const std::string RAW_DOCUMENT_SCHEMA_NAME = "raw_doc_schema_name";
const std::string SRC_BATCH_MODE = "batch_mode";
const std::string CIPHER_FILE_READER_PARAM = "cipher_parameter";
const std::string CIPHER_PARAMETER_ENABLE_BASE64 = "cipher_parameter_enable_base64";
const std::string FILTER_READER = "filter_reader";
const std::string FILTER_FIELD_NAME = "filter_field_name";
const std::string FILTER_TYPE = "filter_type";
const std::string FILTER_VALUE = "filter_value";

const std::string FILE_READ_SRC = "file";
const std::string INDEX_READ_SRC = "index";
const std::string INDEX_TO_FILE_READ_SRC = "index_to_file";
const std::string FIX_LEN_BINARY_FILE_READ_SRC = "fix_length_binary_file";
const std::string VAR_LEN_BINARY_FILE_READ_SRC = "var_length_binary_file";
const std::string INDEX_DOCUMENT_READ_SRC = "index_document";
const std::string KV_DOCUMENT_READ_SRC = "kv_document";
const std::string KV_DOCUMENT_READ_SRC_TO_ODPS = "kv_document_to_odps";
const std::string SWIFT_READ_SRC = "swift";
const std::string SWIFT_TOPIC_STREAM_MODE = "topic_stream_mode";
const std::string SWIFT_PROCESSED_READ_SRC = "swift_processed_topic";
const std::string PLUGIN_READ_SRC = "plugin";
const std::string HOLOGRES_READ_SRC = "hologres";
const std::string READ_SRC_SEPARATOR = ",";

const std::string INDEX_CLONE_SRC = "index_clone";
const std::string SOURCE_INDEX_ADMIN_ADDRESS = "source_admin_address";
const std::string SOURCE_INDEX_BUILD_ID = "source_build_id";
const std::string MADROX_ADMIN_ADDRESS = "madrox_admin_address";
const std::string INDEX_CLONE_LOCATOR = "cloned-index-locator";

const std::string EXTEND_FIELD_NAME = "extend_field_name";
const std::string EXTEND_FIELD_VALUE = "extend_field_value";
const std::string BS_RAW_FILES_META = "bs_raw_file_meta";
const std::string BS_RAW_FILE_META_DIR = "bs_raw_files_meta_dir";
const std::string ENABLE_INGESTION_TIMESTAMP = "enable_ingestion_timestamp";

const std::string RAW_DOCUMENT_FILE_PATTERN = "file_pattern";
const std::string TRACE_FIELD = "trace_field";
const std::string TIMESTAMP_FIELD = "timestamp_field";
const std::string SOURCE_FIELD = "source_field";
const std::string SOURCE_DEFAULT = "source_default";
const std::string SRC_SIGNATURE = "src_signature";
const std::string LOCATOR_USER_DATA = "locator_user_data";
const std::string NEED_FIELD_FILTER = "need_field_filter";
const std::string HA_RESERVED_SOURCE = "ha_reserved_source";
const std::string USE_FIELD_SCHEMA = "use_field_schema";
const std::string NEED_PRINT_DOC_TAG = "need_print_doc_tag";
const std::string KEEP_PARSER_ORDER = "keep_parser_order";

const std::string SWIFT_ZOOKEEPER_ROOT = "swift_root";
const std::string SWIFT_TOPIC_NAME = "topic_name";
const std::string SWIFT_READER_CONFIG = "reader_config";
const std::string SWIFT_WRITER_CONFIG = "writer_config";
const std::string SWIFT_CLIENT_CONFIG = "swift_client_config";
const std::string DEFAULT_SWIFT_BROKER_TOPIC_CONFIG = "topic";
const std::string FULL_SWIFT_BROKER_TOPIC_CONFIG = "topic_full";
const std::string SWIFT_TOPIC_CONFIGS = "swift_topic_configs";
const std::string INC_SWIFT_BROKER_TOPIC_CONFIG = "topic_inc";
const std::string RAW_SWIFT_TOPIC_CONFIG = "topic_raw";
const std::string RAW_TOPIC_SWIFT_READER_CONFIG = "raw_topic_reader_config";

const std::string READ_INDEX_ROOT = "read_index_path";
const std::string READ_INDEX_VERSION = "read_index_version";
const std::string PREFER_SOURCE_INDEX = "prefer_source_index";
const std::string USER_DEFINE_INDEX_PARAM = "user_define_index_param";
const std::string READ_IGNORE_ERROR = "read_ignore_error";
const std::string READ_INDEX_CACHE_SIZE = "read_index_block_cache_size_in_mb";
const std::string READ_CACHE_BLOCK_SIZE = "read_cache_block_size_in_mb";
const std::string USER_REQUIRED_FIELDS = "read_index_required_fields";
const std::string READER_TIMESTAMP = "reader_timestamp";

const std::string RAW_DOCUMENT_QUERY_STRING = "raw_doc_query_string";
const std::string RAW_DOCUMENT_QUERY_TYPE = "raw_doc_query_type";
const std::string READER_ENABLE_RAW_DOCUMENT_QUERY = "reader_enable_raw_doc_query";
const std::string READER_ENABLE_STRICT_EVALUATE = "reader_enable_strict_evaluate";

const std::string READ_THROUGH_DCACHE = "read_through_dcache";

// worker common
const std::string RESERVED_CLUSTER_CHECKPOINT_LIST = "reserved_checkpoint_list";
const std::string RESERVED_VERSION_LIST = "reserved_version_list";
const std::string BATCH_MASK = "batch_mask";
const std::string USE_INNER_BATCH_MASK_FILTER = "use_inner_batch_mask_filter";
const std::string IS_BATCH_MODE = "is_batch_mode";
const std::string ENABLE_FAST_SLOW_QUEUE = "enableFastSlowQueue";
const std::string RAW_TOPIC_FAST_SLOW_QUEUE_MASK = "swift_raw_topic_fast_slow_queue_mask";
const std::string RAW_TOPIC_FAST_SLOW_QUEUE_RESULT = "swift_raw_topic_fast_slow_queue_result";
const std::string SWIFT_FILTER_MASK = "swift_filter_mask";
const std::string SWIFT_FILTER_RESULT = "swift_filter_result";
const std::string FAST_QUEUE_SWIFT_FILTER_MASK = "fast_queue_swift_filter_mask";
const std::string FAST_QUEUE_SWIFT_FILTER_RESULT = "fast_queue_swift_filter_result";
const std::string PROCESSOR_CONFIG_NODE = "processor_config_node";
const std::string INDEX_VERSION = "resource_index_version";
const std::string INDEX_CLUSTER = "resource_index_cluster";
const std::string RESOURCE_INDEX_ROOT = "resource_index_root";
const std::string RESOURCE_BACKUP_INDEX_ROOT = "resource_backup_index_root";
const std::string BS_RESOURCE_READER = "bs_resource_reader";

// worker input & output
const std::string INPUT_CONFIG = "InputConfig";
const std::string INPUT_DOC_TYPE = "input_doc_type";
const std::string INPUT_DOC_RAW = "input_doc_raw";
const std::string INPUT_DOC_PROCESSED = "input_doc_processed";
const std::string INPUT_DOC_BATCHDOC = "input_doc_batchdoc";
const std::string WORKER_HAS_OUTPUT = "has_output";

// processor
const std::string DATA_TABLE_NAME = "data_table";
const std::string DATA_DESCRIPTION_KEY = "data_description";
const std::string DATA_DESCRIPTION_PARSER_CONFIG = "parser_config";
const std::string SRC_RESOURCE_NAME = "_src_latestCheckpoint";
const std::string SYNC_LOCATOR_FROM_COUNTER = "sync_locator_from_counter";

// region config
const std::string DEFAULT_REGION_FIELD_NAME = "ha_reserved_region_field";
const std::string REGION_DISPATCHER_CONFIG = "dispatcher_type";
const std::string FIELD_DISPATCHER_TYPE = "dispatch_by_field";
const std::string REGION_FIELD_NAME_CONFIG = "region_field_name";
const std::string REGION_ID_DISPATCHER_TYPE = "dispatch_by_id";
const std::string REGION_ID_CONFIG = "region_id";
const std::string ALL_REGION_DISPATCHER_TYPE = "dispatch_to_all_region";

// processor::swift
const std::string PROCESSED_DOC_SWIFT_ROOT = "processed_doc_swift_root";
const std::string PROCESSED_DOC_SWIFT_TOPIC_PREFIX = "processed_swift_topic_prefix";
const std::string PROCESSED_DOC_SWIFT_TOPIC_NAME = "processed_swift_topic_name";
const std::string PROCESSED_DOC_SWIFT_READER_CONFIG = "processed_swift_reader_config";
const std::string PROCESSED_DOC_SWIFT_WRITER_CONFIG = "processed_swift_writer_config";
const std::string ALLOW_SEEK_CROSS_SRC = "allow_seek_cross_src";
const std::string CHECKPOINT_ROOT = "checkpoint_root";
const std::string CHECKPOINT = "checkpoint";
const std::string CONSUMER_CHECKPOINT = "consumer_checkpoint";
const std::string SWIFT_START_TIMESTAMP = "swift_start_timestamp";
const std::string SWIFT_STOP_TIMESTAMP = "swift_stop_timestamp";
const std::string SWIFT_SUSPEND_TIMESTAMP = "swift_suspend_timestamp";
const std::string SWIFT_MAJOR_VERSION = "swift_major_version";
const std::string SWIFT_MINOR_VERSION = "swift_minor_version";
const std::string SWIFT_PROCESSOR_WRITER_NAME = "swift_processor_name";

// builder
const std::string VERSION_TIMESTAMP = "version_timestamp";
const std::string INDEX_ROOT_PATH = "index_root"; // required
const std::string SHARED_TOPIC_CLUSTER_NAME = "shared_topic_cluster_name";
const std::string BUILDER_INDEX_ROOT_PATH = "builder_index_root";
const std::string CLUSTER_NAMES = "cluster_names"; // required
const std::string GENERATION_ID = "generation";
const std::string CLUSTER_NAME_SEPARATOR = ",";
const std::string CLUSTER_INFO_SEPARATOR = ":";
const std::string BUILD_MODE = "build_mode";
const std::string BUILD_MODE_FULL = "full";
const std::string BUILD_MODE_INCREMENTAL = "incremental";
const std::string BUILD_MODE_DEFAULT = BUILD_MODE_FULL;
const std::string BUILD_STEP_FULL_STR = "full";
const std::string BUILD_STEP_INC_STR = "incremental";
const std::string PARTITION_COUNT = "partition_count";
const std::string BUILD_PARTITION_FROM = "build_partition_from";
const std::string BUILD_PARTITION_COUNT = "build_partition_count";
const std::string PROCESSED_DOC_SWIFT_STOP_TIMESTAMP = "processed_doc_swift_stop_timestamp";
const std::string BUILD_VERSION_TIMESTAMP = "build_version_timestamp";
const std::string OPERATION_IDS = "operation_ids";

// job
const std::string BUILD_PARALLEL_NUM = "build_parallel_num";
const std::string MERGE_PARALLEL_NUM = "merge_parallel_num";
const std::string MAP_REDUCE_RATIO = "map_reduce_ratio";
const std::string PART_SPLIT_NUM = "partition_split_num";
const std::string NEED_PARTITION = "need_partition";
const std::string MERGE_CONFIG_NAME = "merge_config_name";
const std::string MERGE_TIMESTAMP = "merge_timestamp";
const std::string ALIGNED_VERSION_ID = "aligned_versionid";
const std::string TARGET_DESCRIPTION = "target_description";

const std::string DATA_LINK_MODE = "data_link_mode";
}} // namespace build_service::config
