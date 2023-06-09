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

#include <memory>
#include <string>

#include "indexlib/base/Constant.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kv/Constant.h"
#include "indexlib/index/primary_key/Constant.h"
#include "indexlib/index/source/Constant.h"
#include "indexlib/index/summary/Constant.h"

namespace indexlib { namespace index {

#define ATTRIBUTE_UPDATE_INFO_FILE_NAME "update_info"
#define INDEX_SUMMARY_FILE_PREFIX "index_summary"

#define SUMMARY_DIR_NAME "summary"
#define SOURCE_DIR_NAME "source"
#define DELETION_MAP_DIR_NAME "deletionmap"
#define PK_ATTRIBUTE_DIR_NAME_PREFIX "attribute"
#define OPERATION_DIR_NAME "operation_log"
#define REALTIME_TIMESTAMP_FILE "rt_doc_timestamp"
#define SHARDING_DIR_NAME_PREFIX "column"
#define PARTITION_PATCH_META_FILE_PREFIX "patch_meta"
#define ALTER_FIELD_CHECK_POINT_DIR_NAME "checkpoint_dir"
#define MERGE_ITEM_CHECK_POINT_DIR_NAME "check_points"
#define INDEX_SUMMARY_INFO_DIR_NAME "summary_info"

#define DELETION_MAP_FILE_NAME_PREFIX "data"
#define OPERATION_DATA_FILE_NAME "data"
#define OPERATION_META_FILE_NAME "meta"
#define TRIE_ORIGINAL_DATA "original_data"

#define BRANCH_DIR_NAME_PREFIX "__BRANCH__"
#define MARKED_BRANCH_INFO "__MARKED_BRANCH_INFO"
#define SEGMENT_TEMP_DIR_SUFFIX "_tmp/"

#define SWAP_MMAP_FILE_NAME "swap_mmap_file"
#define NEED_STORE_PKEY_VALUE "NEED_STORE_PKEY_VALUE"

#define TRUNCATE_INDEX_DIR_NAME "truncate"

#define MAX_RECORD_SIZE 128
#define MAX_TOKENATTR_COUNT_PER_DOC 512

#define REALTIME_MERGE_STRATEGY_STR "realtime"
#define BALANCE_TREE_MERGE_STRATEGY_STR "balance_tree"
#define OPTIMIZE_MERGE_STRATEGY_STR "optimize"
#define FRESHNESS_MERGE_STRATEGY_STR "freshness"
#define SPECIFIC_SEGMENTS_MERGE_STRATEGY_STR "specific_segments"
#define PRIORITY_QUEUE_MERGE_STRATEGY_STR "priority_queue"
#define SHARD_BASED_MERGE_STRATEGY_STR "shard_based"
#define ALIGN_VERSION_MERGE_STRATEGY_STR "align_version"
#define TIMESERIES_MERGE_STRATEGY_STR "time_series"
#define TEMPERATURE_MERGE_STRATEGY_STR "temperature"
#define LARGE_TIME_RANGE_SELECTION_MERGE_STRATEGY_STR "large_time_range_selection"
#define SEGMENT_COUNT_MERGE_STRATEGY_STR "segment_count"

#define TEMPERATURE_SEGMENT_METIRC_KRY "temperature"

#define JOIN_INDEX_PARTITION_DIR_NAME "join_index_partition"
#define SUB_SEGMENT_DIR_NAME "sub_segment"

#define SEGMENT_MEM_CONTROL_GROUP "segment_memory_control"
#define SEGMENT_INIT_MEM_USE_RATIO "init_memory_use_ratio"

#define TIME_TO_NOW_FUNCTION "time_to_now"

enum PositionListType {
    pl_pack,
    pl_text,
    pl_expack,
    pl_bitmap,
};

enum SortType { st_dsc, st_asc };

inline const std::string VIRTUAL_TIMESTAMP_FIELD_NAME = "virtual_timestamp_field";
inline const std::string VIRTUAL_TIMESTAMP_INDEX_NAME = "virtual_timestamp_index";

inline const std::string DELETION_MAP_TASK_NAME = "deletionmap";
inline const std::string INDEX_TASK_NAME = "index";
inline const std::string SUMMARY_TASK_NAME = "summary";
inline const std::string SOURCE_TASK_NAME = "source";
inline const std::string ATTRIBUTE_TASK_NAME = "attribute";
inline const std::string PACK_ATTRIBUTE_TASK_NAME = "pack_attribute";
inline const std::string KEY_VALUE_TASK_NAME = "key_value";

inline const std::string ATTRIBUTE_IDENTIFIER = "attribute";
inline const std::string CHECKED_DOC_COUNT = "checked_doc_count";
inline const std::string LIFECYCLE = "lifecycle";
inline const std::string LIFECYCLE_DETAIL = "lifecycle_detail";

inline const std::string BASELINE_IDENTIFIER = "baseline";

constexpr int64_t INDEX_CACHE_DEFAULT_BLOCK_SIZE = 64 * 1024;  // default block size for index cache
constexpr int64_t SUMMARY_CACHE_DEFAULT_BLOCK_SIZE = 4 * 1024; // default block size for summary cache

constexpr double EPSINON = 0.00001;

enum TemperatureProperty {
    HOT = 0,
    WARM,
    COLD,
    UNKNOWN,
};

inline uint64_t MicrosecondToSecond(uint64_t microsecond)
{
    return microsecond / 1000000;
} // autil::TimeUtility::us2sec
inline uint64_t SecondToMicrosecond(uint64_t second) { return second * 1000000; } // autil::TimeUtility::sec2us
}}                                                                                // namespace indexlib::index
