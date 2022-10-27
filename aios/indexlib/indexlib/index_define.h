#ifndef __INDEXLIB_INDEX_DEFINE_H
#define __INDEXLIB_INDEX_DEFINE_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>

IE_NAMESPACE_BEGIN(index);

#define SEGMENT_INFO_FILE_NAME "segment_info"
#define SEGMENT_FILE_NAME_PREFIX "segment"
#define VERSION_FILE_NAME_PREFIX "version"
#define DEPLOY_META_FILE_NAME_PREFIX "deploy_meta"
static const std::string SCHEMA_FILE_NAME = "schema.json";
#define INDEX_FORMAT_VERSION_FILE_NAME "index_format_version"
#define INDEX_FORMAT_VERSION_KEY "index_format_version"
#define INDEX_PARTITION_META_FILE_NAME "index_partition_meta"
#define INDEX_BASE_TIMESTAMP_FILE_NAME "index_base_timestamp"
#define DEPLOY_INDEX_FILE_NAME "deploy_index"
#define SEGMENT_FILE_LIST "segment_file_list"
#define ATTRIBUTE_UPDATE_INFO_FILE_NAME "update_info"
#define ATTRIBUTE_DATA_INFO_FILE_NAME "data_info"
#define INDEX_SUMMARY_FILE_PREFIX "index_summary"

#define INDEX_DIR_NAME "index"
#define SUMMARY_DIR_NAME "summary"
#define ATTRIBUTE_DIR_NAME "attribute"
#define DELETION_MAP_DIR_NAME "deletionmap"
#define PK_ATTRIBUTE_DIR_NAME_PREFIX "attribute"
#define OPERATION_DIR_NAME "operation_log"
#define TRUNCATE_META_DIR_NAME "truncate_meta"
#define ADAPTIVE_DICT_DIR_NAME "adaptive_bitmap_meta"
#define REALTIME_TIMESTAMP_FILE "rt_doc_timestamp"
#define SHARDING_DIR_NAME_PREFIX "column"
#define PARTITION_PATCH_DIR_PREFIX "patch_index"
#define PARTITION_PATCH_META_FILE_PREFIX "patch_meta"
#define MERGE_ITEM_CHECK_POINT_DIR_NAME "check_points"
#define INDEX_SUMMARY_INFO_DIR_NAME "summary_info"

#define RT_INDEX_PARTITION_DIR_NAME "rt_index_partition" 
#define JOIN_INDEX_PARTITION_DIR_NAME "join_index_partition" 

#define DICTIONARY_FILE_NAME "dictionary"
#define POSTING_FILE_NAME "posting"
#define BITMAP_DICTIONARY_FILE_NAME "bitmap_dictionary"
#define BITMAP_POSTING_FILE_NAME "bitmap_posting"
#define INDEX_FORMAT_OPTION_FILE_NAME "index_format_option"
#define SEGMETN_METRICS_FILE_NAME "segment_metrics"
#define SEGMENT_CUSTOMIZE_METRICS_GROUP "customize"

#define SUMMARY_OFFSET_FILE_NAME "offset"
#define SUMMARY_DATA_FILE_NAME "data"
#define ATTRIBUTE_DATA_FILE_NAME "data"
#define ATTRIBUTE_DATA_EXTEND_SLICE_FILE_NAME "extend_slice_data"
#define ATTRIBUTE_OFFSET_FILE_NAME "offset"
#define ATTRIBUTE_PATCH_FILE_NAME "patch"
#define COMPRESS_PATCH_META_FILE_NAME "compress_patch_meta"
#define PRIMARY_KEY_DATA_FILE_NAME "data"
#define PRIMARY_KEY_DATA_SLICE_FILE_NAME "slice_data"
#define DELETION_MAP_FILE_NAME_PREFIX "data"
#define OPERATION_DATA_FILE_NAME "data"
#define OPERATION_META_FILE_NAME "meta"
#define TRIE_ORIGINAL_DATA "original_data"
#define COUNTER_FILE_NAME "counter"

#define SEGMENT_TEMP_DIR_SUFFIX "_tmp/"

#define KV_KEY_FILE_NAME "key"
#define KV_VALUE_FILE_NAME "value"
#define KV_FORMAT_OPTION_FILE_NAME "format_option"

#define PREFIX_KEY_FILE_NAME "pkey"
#define SUFFIX_KEY_FILE_NAME "skey"
#define KKV_VALUE_FILE_NAME "value"
#define KKV_INDEX_FORMAT_FILE "kkv_index_format"
#define SWAP_MMAP_FILE_NAME "swap_mmap_file"
#define RANGE_INFO_FILE_NAME "range_info"

#define ATTRIBUTE_OFFSET_EXTEND_SUFFIX ".extend64";
#define EQUAL_COMPRESS_UPDATE_EXTEND_SUFFIX  ".extend_equal_compress";

#define TRUNCATE_INDEX_DIR_NAME "truncate"

const size_t EQUAL_COMPRESS_UPDATE_SLICE_LEN = 64 * 1024 * 1024;
const size_t EQUAL_COMPRESS_UPDATE_MAX_SLICE_COUNT = 1024;

const size_t VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD = 0xFFFFFFFFL;

#define MAX_RECORD_SIZE 128
#define MAX_SECTION_COUNT_PER_DOC (255 * 32)
#define MAX_SECTION_BUFFER_LEN (MAX_SECTION_COUNT_PER_DOC * 5 + 4)
#define MAX_TOKENATTR_COUNT_PER_DOC 512

#define SECTION_TAIL_MAGIC 0xF1F2F3F4
#define UINT32_OFFSET_TAIL_MAGIC 0xF2F3F4F5
#define UINT64_OFFSET_TAIL_MAGIC 0xF3F4F5F6

#define REALTIME_MERGE_STRATEGY_STR "realtime"
#define BALANCE_TREE_MERGE_STRATEGY_STR "balance_tree"
#define OPTIMIZE_MERGE_STRATEGY_STR "optimize"
#define FRESHNESS_MERGE_STRATEGY_STR "freshness"
#define SPECIFIC_SEGMENTS_MERGE_STRATEGY_STR "specific_segments"
#define PRIORITY_QUEUE_MERGE_STRATEGY_STR "priority_queue"
#define SHARD_BASED_MERGE_STRATEGY_STR "shard_based"
#define ALIGN_VERSION_MERGE_STRATEGY_STR "align_version"
#define TIMESERIES_MERGE_STRATEGY_STR "time_series"
#define LARGE_TIME_RANGE_SELECTION_MERGE_STRATEGY_STR "large_time_range_selection"

#define RT_INDEX_PARTITION_DIR_NAME "rt_index_partition" 
#define JOIN_INDEX_PARTITION_DIR_NAME "join_index_partition" 
#define SUB_SEGMENT_DIR_NAME "sub_segment"

#define SEGMENT_MEM_CONTROL_GROUP "segment_memory_control"
#define SEGMENT_INIT_MEM_USE_RATIO "init_memory_use_ratio"

enum PositionListType
{
    pl_pack,
    pl_text,
    pl_expack,
    pl_bitmap,
};

enum SortType
{
    st_dsc,
    st_asc
};

const std::string VIRTUAL_TIMESTAMP_FIELD_NAME = "virtual_timestamp_field";
const std::string VIRTUAL_TIMESTAMP_INDEX_NAME = "virtual_timestamp_index";
static const FieldType VIRTUAL_TIMESTAMP_FIELD_TYPE = ft_int64;
static const IndexType VIRTUAL_TIMESTAMP_INDEX_TYPE = it_number_int64;

static const std::string DELETION_MAP_TASK_NAME = "deletionmap";
static const std::string INDEX_TASK_NAME = "index";
static const std::string SUMMARY_TASK_NAME = "summary";
static const std::string ATTRIBUTE_TASK_NAME = "attribute";
static const std::string PACK_ATTRIBUTE_TASK_NAME = "pack_attribute";
static const std::string KEY_VALUE_TASK_NAME = "key_value";

static const std::string ATTRIBUTE_IDENTIFIER = "attribute";
static const std::string CHECKED_DOC_COUNT = "checked_doc_count";
static const std::string LIFECYCLE = "lifecycle";

static const std::string BASELINE_IDENTIFIER = "baseline";

const int64_t INDEX_CACHE_DEFAULT_BLOCK_SIZE = 64 * 1024; // default block size for index cache
const int64_t SUMMARY_CACHE_DEFAULT_BLOCK_SIZE = 4 * 1024; // default block size for summary cache

const double EPSINON = 0.00001;

const uint32_t MAX_UNCOMPRESSED_SKIP_LIST_SIZE = 10;

const uint32_t MAX_UNCOMPRESSED_DOC_LIST_SIZE = 5;

const uint32_t MAX_UNCOMPRESSED_POS_LIST_SIZE = 5;
const uint8_t COMPRESS_MODE_SIZE = 4;

//now we only support 4 compress mode
enum CompressMode
{
    PFOR_DELTA_COMPRESS_MODE = 0x00,
    REFERENCE_COMPRESS_MODE = 0x01,
    DICT_INLINE_COMPRESS_MODE = 0x02,
    SHORT_LIST_COMPRESS_MODE = 0x03,
};

// used 4 bit.
const uint64_t OFFSET_MARK = 0x0fffffffffffffff;

inline uint64_t MicrosecondToSecond(uint64_t microsecond)
{
    return microsecond / 1000000;
}
inline uint64_t SecondToMicrosecond(uint64_t second)
{
    return second * 1000000;
}

IE_NAMESPACE_END(index);

#endif //__INDEXENGINEINDEX_DEFINE_H

