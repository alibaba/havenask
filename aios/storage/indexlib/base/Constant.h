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

#include <limits>
#include <string>

#include "indexlib/base/Types.h"

namespace indexlib {

constexpr docid_t INVALID_DOCID = -1;
constexpr globalid_t INVALID_GLOBALID = -1;
constexpr size_t MAX_SEGMENT_DOC_COUNT = (uint64_t)std::numeric_limits<docid32_t>::max() + 1ULL;
constexpr fieldid_t INVALID_FIELDID = -1;
constexpr uint32_t DOCUMENT_BINARY_VERSION = 12;
constexpr uint32_t LEGACY_DOCUMENT_BINARY_VERSION = 10;
constexpr uint32_t UNINITIALIZED_TTL = 0;
constexpr size_t DEFAULT_CHUNK_SIZE = 10; // 10MB
constexpr uint32_t MAX_SECTION_COUNT_PER_DOC = 255 * 32;
constexpr uint32_t MAX_SECTION_BUFFER_LEN = MAX_SECTION_COUNT_PER_DOC * 5 + 4;
constexpr uint32_t REPORT_METRICS_INTERVAL = 1 * 1000 * 1000;
constexpr int64_t INVALID_TIMESTAMP = -1;
constexpr versionid_t INVALID_VERSIONID = -1;
constexpr versionid_t CONTROL_FLOW_VERSIONID = -2;
constexpr segmentid_t INVALID_SEGMENTID = -1;
constexpr size_t MAX_RECORD_SIZE = 128;
constexpr schema_opid_t INVALID_SCHEMA_OP_ID = (schema_opid_t)0;
constexpr uint32_t INVALID_KEEP_VERSION_COUNT = -1;
constexpr schemaid_t DEFAULT_SCHEMAID = (indexlib::schemaid_t)0;
constexpr schemaid_t INVALID_SCHEMAID = -1;
constexpr size_t MAX_SPLIT_SEGMENT_COUNT = 1lu << 16;
constexpr uint32_t DEFAULT_CHUNK_SIZE_BYTES = DEFAULT_CHUNK_SIZE * 1024 * 1024;
constexpr size_t HASHMAP_INIT_SIZE = 1024;
constexpr int64_t MAX_BRANCH_COUNT = 100;
constexpr uint32_t INVALID_DOC_VERSION = -1;
constexpr char MULTI_VALUE_SEPARATOR = '\x1D'; // equal with autil::MULTI_VALUE_DELIMITER;

static constexpr int64_t DEFAULT_DONE_TASK_TTL_IN_SECONDS = 24 * 3600 * 5;
static constexpr int64_t INVALID_TTL = -1;
// default time to live in seconds, make sure not overflow when converted to microsecond
static constexpr int64_t DEFAULT_TIME_TO_LIVE = std::numeric_limits<int64_t>::max() >> 20;

inline const std::string INDEXLIB_DEFAULT_DELIM_STR_LEGACY = " ";
inline const std::string INDEXLIB_MULTI_VALUE_SEPARATOR_STR(1, MULTI_VALUE_SEPARATOR);
inline const std::string VERSION_FILE_NAME_PREFIX = "version";
inline const std::string VERSION_FILE_NAME_DOT_PREFIX = "version.";
inline const std::string SCHEMA_FILE_NAME = "schema.json";
inline const std::string SEGMENT_INFO_FILE_NAME = "segment_info";
inline const std::string DEPLOY_INDEX_FILE_NAME = "deploy_index"; // legacy
inline const std::string SEGMENT_FILE_LIST = "segment_file_list"; // legacy
inline const std::string RT_INDEX_PARTITION_DIR_NAME = "rt_index_partition";
inline const std::string JOIN_INDEX_PARTITION_DIR_NAME = "join_index_partition"; // legacy
inline const std::string COUNTER_FILE_NAME = "counter";
inline const std::string DOCUMENT_SOURCE_TAG_KEY = "__source__";
inline const std::string DOCUMENT_HASHID_TAG_KEY = "__hash_id__";
inline const std::string DOC_TIME_TO_LIVE_IN_SECONDS = "doc_time_to_live_in_seconds";
inline const std::string SEGMENT_INIT_TIMESTAMP = "segment_init_timestamp";
inline const std::string HA_RESERVED_TIMESTAMP = "ha_reserved_timestamp";
inline const std::string DEFAULT_NULL_FIELD_STRING = "__NULL__";
inline const std::string DOCUMENT_INGESTIONTIMESTAMP_TAG_KEY = "__ingestion_timestamp__";
inline const std::string PATCH_INDEX_DIR_PREFIX = "patch_index_";
inline const std::string LEGACY_TABLE_TYPE = "__LEGACY_TABLE__"; // legacy

static constexpr const char* INDEX_FORMAT_VERSION = "2.1.2"; // legacy, 2.1.2 in indexlib.h
static constexpr const char* SEGMENT_FILE_NAME_PREFIX = "segment";
static constexpr const char* ENTRY_TABLE_FILE_NAME_PREFIX = "entry_table";
static constexpr const char* INDEX_FORMAT_VERSION_FILE_NAME = "index_format_version";
static constexpr const char* INDEX_PARTITION_META_FILE_NAME = "index_partition_meta"; // legacy
static constexpr const char* DEPLOY_META_FILE_NAME_PREFIX = "deploy_meta";            // legacy
static constexpr const char* FENCE_DIR_NAME_PREFIX = "__FENCE__";
static constexpr const char* FENCE_LEASE_FILE_NAME = "FENCE_LEASE";
static constexpr const char* PARALLEL_BUILD_INFO_FILE = "parallel_build_info_file"; // legacy
static constexpr const char* TRUNCATE_META_DIR_NAME = "truncate_meta";
static constexpr const char* ADAPTIVE_DICT_DIR_NAME = "adaptive_bitmap_meta";
static constexpr const char* SEGMETN_METRICS_FILE_NAME = "segment_metrics";
static constexpr const char* TASK_FENCE_DIR_NAME_PREFIX = "TASK_";
static constexpr const char* NORMAL_TABLE_META_INFO_IDENTIFIER = "normal_table_meta_info";
static constexpr const char* COUNTER_PREFIX = "online.access.";
static constexpr const char* PATCH_FILE_NAME = "patch";
static constexpr const char* FILE_COMPRESS = "file_compress";
static constexpr const char* SEGMENT_CUSTOMIZE_METRICS_GROUP = "customize";
static constexpr const char* SEGMENT_COMPRESS_FINGER_PRINT = "compress_finger_print";

} // namespace indexlib

//////////////////////////////////////////////////////////////////////
// TODO: rm
namespace indexlibv2 {
using indexlib::ADAPTIVE_DICT_DIR_NAME;
using indexlib::CONTROL_FLOW_VERSIONID;
using indexlib::COUNTER_FILE_NAME;
using indexlib::COUNTER_PREFIX;
using indexlib::DEFAULT_CHUNK_SIZE;
using indexlib::DEFAULT_CHUNK_SIZE_BYTES;
using indexlib::DEFAULT_NULL_FIELD_STRING;
using indexlib::DEFAULT_SCHEMAID;
using indexlib::DEPLOY_META_FILE_NAME_PREFIX;
using indexlib::DOC_TIME_TO_LIVE_IN_SECONDS;
using indexlib::DOCUMENT_BINARY_VERSION;
using indexlib::DOCUMENT_HASHID_TAG_KEY;
using indexlib::DOCUMENT_INGESTIONTIMESTAMP_TAG_KEY;
using indexlib::DOCUMENT_SOURCE_TAG_KEY;
using indexlib::ENTRY_TABLE_FILE_NAME_PREFIX;
using indexlib::FENCE_DIR_NAME_PREFIX;
using indexlib::FENCE_LEASE_FILE_NAME;
using indexlib::FILE_COMPRESS;
using indexlib::HASHMAP_INIT_SIZE;
using indexlib::INDEX_FORMAT_VERSION_FILE_NAME;
using indexlib::INDEX_PARTITION_META_FILE_NAME;
using indexlib::INDEXLIB_MULTI_VALUE_SEPARATOR_STR;
using indexlib::INVALID_DOC_VERSION;
using indexlib::INVALID_DOCID;
using indexlib::INVALID_FIELDID;
using indexlib::INVALID_GLOBALID;
using indexlib::INVALID_KEEP_VERSION_COUNT;
using indexlib::INVALID_SCHEMAID;
using indexlib::INVALID_SEGMENTID;
using indexlib::INVALID_TIMESTAMP;
using indexlib::INVALID_VERSIONID;
using indexlib::LEGACY_DOCUMENT_BINARY_VERSION;
using indexlib::MAX_BRANCH_COUNT;
using indexlib::MAX_RECORD_SIZE;
using indexlib::MAX_SECTION_BUFFER_LEN;
using indexlib::MAX_SECTION_COUNT_PER_DOC;
using indexlib::MAX_SPLIT_SEGMENT_COUNT;
using indexlib::MULTI_VALUE_SEPARATOR;
using indexlib::PARALLEL_BUILD_INFO_FILE;
using indexlib::PATCH_FILE_NAME;
using indexlib::REPORT_METRICS_INTERVAL;
using indexlib::RT_INDEX_PARTITION_DIR_NAME;
using indexlib::SCHEMA_FILE_NAME;
using indexlib::SEGMENT_COMPRESS_FINGER_PRINT;
using indexlib::SEGMENT_CUSTOMIZE_METRICS_GROUP;
using indexlib::SEGMENT_FILE_NAME_PREFIX;
using indexlib::SEGMENT_INFO_FILE_NAME;
using indexlib::TASK_FENCE_DIR_NAME_PREFIX;
using indexlib::TRUNCATE_META_DIR_NAME;
using indexlib::UNINITIALIZED_TTL;
using indexlib::VERSION_FILE_NAME_PREFIX;
} // namespace indexlibv2
