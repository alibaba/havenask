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
#pragma once
#include <string>

namespace build_service { namespace config {

// constant
extern const std::string KV_TRUE;
extern const std::string KV_FALSE;

// help
extern const std::string HELP;

// common
extern const std::string CONFIG_PATH;     // required
extern const std::string COUNTER_ZK_ROOT; // required
extern const std::string RANGE_FROM;
extern const std::string RANGE_TO;
extern const std::string ZOOKEEPER_ROOT;
extern const std::string COUNTER_CONFIG_JSON_STR;
extern const std::string SAFE_WRITE;

// monitor
extern const std::string USER_NAME;
extern const std::string SERVICE_NAME;
extern const std::string AMON_PORT;

// worker
extern const std::string ALOG_CONF;
extern const std::string WORKDIR;
extern const std::string ROLE;
extern const std::string WORKER_PATH_VERSION;

// realtime
extern const std::string BS_SERVER_ADDRESS;
extern const std::string REALTIME_MODE;
extern const std::string REALTIME_SERVICE_MODE;
extern const std::string REALTIME_JOB_MODE;
extern const std::string REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE;
extern const std::string REALTIME_PROCESS_RAWDOC;

// reader
extern const std::string DATA_PATH; // required
extern const std::string READ_SRC;  // required
extern const std::string READ_SRC_CONFIG;
extern const std::string READ_SRC_TYPE;
extern const std::string READ_STREAMING_SOURCE;
extern const std::string READ_SRC_MODULE_NAME;
extern const std::string READ_SRC_MODULE_PATH;
extern const std::string SWIFT_SRC_FULL_BUILD_LATENCY;
extern const std::string SWIFT_SRC_FULL_BUILD_TIMEOUT;
extern const std::string CUSTOMIZE_FULL_PROCESSOR_COUNT;
extern const std::string RAW_DOCUMENT_SCHEMA_NAME;
extern const std::string SRC_BATCH_MODE;
extern const std::string CIPHER_FILE_READER_PARAM;
extern const std::string CIPHER_PARAMETER_ENABLE_BASE64;

extern const std::string FILE_READ_SRC;
extern const std::string INDEX_READ_SRC;
extern const std::string INDEX_TO_FILE_READ_SRC;
extern const std::string FIX_LEN_BINARY_FILE_READ_SRC;
extern const std::string VAR_LEN_BINARY_FILE_READ_SRC;
extern const std::string INDEX_DOCUMENT_READ_SRC;
extern const std::string KV_DOCUMENT_READ_SRC;
extern const std::string SWIFT_READ_SRC;
extern const std::string SWIFT_TOPIC_STREAM_MODE;
extern const std::string SWIFT_PROCESSED_READ_SRC;
extern const std::string PLUGIN_READ_SRC;
extern const std::string HOLOGRES_READ_SRC;
extern const std::string READ_SRC_SEPARATOR;

extern const std::string INDEX_CLONE_SRC;
extern const std::string SOURCE_INDEX_ADMIN_ADDRESS;
extern const std::string SOURCE_INDEX_BUILD_ID;
extern const std::string MADROX_ADMIN_ADDRESS;
extern const std::string INDEX_CLONE_LOCATOR;

extern const std::string EXTEND_FIELD_NAME;
extern const std::string EXTEND_FIELD_VALUE;
extern const std::string BS_RAW_FILES_META;
extern const std::string BS_RAW_FILE_META_DIR;
extern const std::string ENABLE_INGESTION_TIMESTAMP;

extern const std::string RAW_DOCUMENT_FILE_PATTERN;
extern const std::string TRACE_FIELD;
extern const std::string TIMESTAMP_FIELD;
extern const std::string SOURCE_FIELD;
extern const std::string SOURCE_DEFAULT;
extern const std::string SRC_SIGNATURE;
extern const std::string LOCATOR_USER_DATA;
extern const std::string NEED_FIELD_FILTER;
extern const std::string HA_RESERVED_SOURCE;
extern const std::string USE_FIELD_SCHEMA;
extern const std::string NEED_PRINT_DOC_TAG;

extern const std::string SWIFT_ZOOKEEPER_ROOT;
extern const std::string SWIFT_TOPIC_NAME;
extern const std::string SWIFT_READER_CONFIG;
extern const std::string SWIFT_WRITER_CONFIG;
extern const std::string SWIFT_CLIENT_CONFIG;
extern const std::string DEFAULT_SWIFT_BROKER_TOPIC_CONFIG;
extern const std::string FULL_SWIFT_BROKER_TOPIC_CONFIG;
extern const std::string SWIFT_TOPIC_CONFIGS;
extern const std::string INC_SWIFT_BROKER_TOPIC_CONFIG;
extern const std::string RAW_SWIFT_TOPIC_CONFIG;
extern const std::string RAW_TOPIC_SWIFT_READER_CONFIG;

extern const std::string READ_INDEX_ROOT;
extern const std::string READ_INDEX_VERSION;
extern const std::string PREFER_SOURCE_INDEX;
extern const std::string USER_DEFINE_INDEX_PARAM;
extern const std::string READ_IGNORE_ERROR;
extern const std::string READ_INDEX_CACHE_SIZE;
extern const std::string READ_CACHE_BLOCK_SIZE;
extern const std::string USER_REQUIRED_FIELDS;
extern const std::string READER_TIMESTAMP;

extern const std::string RAW_DOCUMENT_QUERY_STRING;
extern const std::string RAW_DOCUMENT_QUERY_TYPE;
extern const std::string READER_ENABLE_RAW_DOCUMENT_QUERY;
extern const std::string READER_ENABLE_STRICT_EVALUATE;

extern const std::string READ_THROUGH_DCACHE;

// worker common
extern const std::string RESERVED_CLUSTER_CHECKPOINT_LIST;
extern const std::string RESERVED_VERSION_LIST;
extern const std::string BATCH_MASK;
extern const std::string IS_BATCH_MODE;
extern const std::string ENABLE_FAST_SLOW_QUEUE;
extern const std::string USE_INNER_BATCH_MASK_FILTER;
extern const std::string RAW_TOPIC_FAST_SLOW_QUEUE_MASK;
extern const std::string RAW_TOPIC_FAST_SLOW_QUEUE_RESULT;
extern const std::string SWIFT_FILTER_MASK;
extern const std::string SWIFT_FILTER_RESULT;
extern const std::string FAST_QUEUE_SWIFT_FILTER_MASK;
extern const std::string FAST_QUEUE_SWIFT_FILTER_RESULT;
extern const std::string PROCESSOR_CONFIG_NODE;
extern const std::string INDEX_VERSION;
extern const std::string INDEX_CLUSTER;
extern const std::string RESOURCE_INDEX_ROOT;
extern const std::string RESOURCE_BACKUP_INDEX_ROOT;

// worker input & output
extern const std::string INPUT_CONFIG;
extern const std::string INPUT_DOC_TYPE;
extern const std::string INPUT_DOC_RAW;
extern const std::string INPUT_DOC_PROCESSED;
extern const std::string WORKER_HAS_OUTPUT;

// processor
extern const std::string DATA_TABLE_NAME;
extern const std::string DATA_DESCRIPTION_KEY;
extern const std::string DATA_DESCRIPTION_PARSER_CONFIG;
extern const std::string SRC_RESOURCE_NAME;
extern const std::string SYNC_LOCATOR_FROM_COUNTER;

// region config
extern const std::string DEFAULT_REGION_FIELD_NAME;
extern const std::string REGION_DISPATCHER_CONFIG;
extern const std::string FIELD_DISPATCHER_TYPE;
extern const std::string REGION_FIELD_NAME_CONFIG;
extern const std::string REGION_ID_DISPATCHER_TYPE;
extern const std::string REGION_ID_CONFIG;
extern const std::string ALL_REGION_DISPATCHER_TYPE;

// processor::swift
extern const std::string PROCESSED_DOC_SWIFT_ROOT;
extern const std::string PROCESSED_DOC_SWIFT_TOPIC_PREFIX;
extern const std::string PROCESSED_DOC_SWIFT_TOPIC_NAME;
extern const std::string PROCESSED_DOC_SWIFT_READER_CONFIG;
extern const std::string PROCESSED_DOC_SWIFT_WRITER_CONFIG;
extern const std::string ALLOW_SEEK_CROSS_SRC;
extern const std::string CHECKPOINT_ROOT;
extern const std::string CHECKPOINT;
extern const std::string CONSUMER_CHECKPOINT;
extern const std::string SWIFT_START_TIMESTAMP;
extern const std::string SWIFT_STOP_TIMESTAMP;
extern const std::string SWIFT_SUSPEND_TIMESTAMP;
extern const std::string SWIFT_MAJOR_VERSION;
extern const std::string SWIFT_MINOR_VERSION;
extern const std::string SWIFT_PROCESSOR_WRITER_NAME;

// builder
extern const std::string VERSION_TIMESTAMP;
extern const std::string INDEX_ROOT_PATH; // required
extern const std::string SHARED_TOPIC_CLUSTER_NAME;
extern const std::string BUILDER_INDEX_ROOT_PATH;
extern const std::string CLUSTER_NAMES; // required
extern const std::string GENERATION_ID;
extern const std::string CLUSTER_NAME_SEPARATOR;
extern const std::string CLUSTER_INFO_SEPARATOR;
extern const std::string BUILD_MODE;
extern const std::string BUILD_MODE_FULL;
extern const std::string BUILD_MODE_INCREMENTAL;
extern const std::string BUILD_MODE_DEFAULT;
extern const std::string PARTITION_COUNT;
extern const std::string BUILD_PARTITION_FROM;
extern const std::string BUILD_PARTITION_COUNT;
extern const std::string PROCESSED_DOC_SWIFT_STOP_TIMESTAMP;
extern const std::string BUILD_VERSION_TIMESTAMP;
extern const std::string OPERATION_IDS;

// job
extern const std::string BUILD_PARALLEL_NUM;
extern const std::string MERGE_PARALLEL_NUM;
extern const std::string MAP_REDUCE_RATIO;
extern const std::string PART_SPLIT_NUM;
extern const std::string NEED_PARTITION;
extern const std::string MERGE_CONFIG_NAME;
extern const std::string MERGE_TIMESTAMP;
extern const std::string ALIGNED_VERSION_ID;
extern const std::string TARGET_DESCRIPTION;
}} // namespace build_service::config
